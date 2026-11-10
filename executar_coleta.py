"""Executor da coleta final (45 rodadas) - MQTT QoS 0/1 vs HTTP/REST.

Para cada rodada do plano (na ordem sorteada) o script:
  1. confere que as portas 1883 e 8000 estão livres;
  2. inicia a captura de pacotes (tshark, interface de loopback);
  3. sobe o servidor do cenário (Mosquitto + subscriber, ou a API);
  4. executa clientes.executar_rodada com os parâmetros do plano;
  5. confere os arquivos gerados, para a captura e derruba os servidores;
  6. registra o resultado (OK / REVISAR / ERRO) em coleta_progresso.csv.

Pode ser interrompido (Ctrl+C) e retomado: basta rodar o mesmo comando.

Uso (na raiz do projeto, com o venv ativo):
    python executar_coleta.py --plano dados\\plano_rodadas.json --dry-run
    python executar_coleta.py --plano dados\\plano_rodadas.json --max-rodadas 1
    python executar_coleta.py --plano dados\\plano_rodadas.json --max-rodadas 9
"""
import argparse
import csv
import glob
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime
from importlib import metadata

try:
    import psutil
except ImportError:  # CPU não será monitorada
    psutil = None

PORTA_MQTT = 1883
PORTA_HTTP = 8000
INTERVALO_S = 5
PROTOCOLOS = ("http", "mqtt_qos0", "mqtt_qos1")
CARGAS = (10, 50, 100)
REPETICOES = (1, 2, 3, 4, 5)

CAMPOS = ["ordem", "cenario_id", "protocolo", "sensores", "repeticao", "semente",
          "inicio", "fim", "status", "observacoes",
          "cpu_gerador_media", "cpu_gerador_max", "cpu_servidor_media",
          "lat_media_ms", "lat_p95_ms", "lat_p99_ms",
          "perdas", "duplicatas", "agend_max_ms", "agend_acima_50ms"]


def agora():
    return datetime.now().isoformat(timespec="seconds")


# ----------------------------------------------------------------------
# Plano
# ----------------------------------------------------------------------
def carregar_plano(caminho):
    with open(caminho, encoding="utf-8") as f:
        plano = json.load(f)
    erros = []
    if len(plano) != 45:
        erros.append(f"o plano tem {len(plano)} rodadas (esperado: 45)")
    vistos, sementes = set(), {}
    for r in plano:
        chave = (r["cenario_id"], r["repeticao"])
        if chave in vistos:
            erros.append(f"rodada duplicada: {chave}")
        vistos.add(chave)
        sementes.setdefault(r["repeticao"], set()).add(r["semente"])
        if r["protocolo"] not in PROTOCOLOS:
            erros.append(f"protocolo inválido: {r['protocolo']}")
        if r["cenario_id"] != f"{r['protocolo']}_{r['num_sensores']}s":
            erros.append(f"cenario_id inconsistente: {r['cenario_id']}")
    for p in PROTOCOLOS:
        for n in CARGAS:
            for rep in REPETICOES:
                if (f"{p}_{n}s", rep) not in vistos:
                    erros.append(f"falta a rodada {p}_{n}s repetição {rep}")
    for rep, s in sementes.items():
        if len(s) != 1:
            erros.append(f"repetição {rep} com mais de uma semente: {sorted(s)}")
    if erros:
        print("PLANO INVÁLIDO:")
        for e in erros:
            print("  -", e)
        sys.exit(1)
    return plano


# ----------------------------------------------------------------------
# Progresso
# ----------------------------------------------------------------------
def ler_progresso(caminho):
    ultimo = {}
    if os.path.exists(caminho):
        with open(caminho, newline="", encoding="utf-8") as f:
            for linha in csv.DictReader(f):
                ultimo[(linha["cenario_id"], int(linha["repeticao"]))] = linha
    return ultimo


def gravar_progresso(caminho, linha):
    novo = not os.path.exists(caminho)
    with open(caminho, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS)
        if novo:
            w.writeheader()
        w.writerow({k: linha.get(k, "") for k in CAMPOS})


def pendentes(plano, ultimo, repetir_revisar):
    saida = []
    for ordem, r in enumerate(plano, 1):
        st = ultimo.get((r["cenario_id"], r["repeticao"]), {}).get("status")
        if st == "OK":
            continue
        if st == "REVISAR" and not repetir_revisar:
            continue
        saida.append((ordem, r))
    return saida


def arquivar(pasta, cenario_id, rep):
    """Move arquivos de uma tentativa anterior para dados/.../invalidas/."""
    achados = glob.glob(os.path.join(pasta, f"*{cenario_id}_rep{rep}*"))
    achados = [a for a in achados if os.path.isfile(a)]
    if not achados:
        return
    destino = os.path.join(pasta, "invalidas", datetime.now().strftime("%Y%m%d_%H%M%S"))
    os.makedirs(destino, exist_ok=True)
    for a in achados:
        shutil.move(a, os.path.join(destino, os.path.basename(a)))


# ----------------------------------------------------------------------
# Utilidades de processo e rede
# ----------------------------------------------------------------------
def porta_aberta(porta):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex(("172.20.10.10", porta)) == 0


def esperar_porta(porta, aberta=True, limite=20):
    fim = time.time() + limite
    while time.time() < fim:
        if porta_aberta(porta) == aberta:
            return True
        time.sleep(0.3)
    return False


def achar_exe(nome, candidatos):
    caminho = shutil.which(nome)
    if caminho:
        return caminho
    for c in candidatos:
        if os.path.exists(c):
            return c
    return None


def detectar_interface(tshark):
    saida = subprocess.run([tshark, "-D"], capture_output=True, text=True).stdout
    for linha in saida.splitlines():
        if "loopback" in linha.lower():
            return linha.split(".")[0].strip()
    return None


def parar_processo(p, espera=10):
    if p is None or p.poll() is not None:
        return
    try:
        p.terminate()
        p.wait(timeout=espera)
    except Exception:  # noqa: BLE001
        p.kill()


def iniciar_captura(tshark, interface, porta, arquivo):
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    p = subprocess.Popen([tshark, "-i", interface, "-f", f"tcp port {porta}", "-w", arquivo],
                         stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                         text=True, creationflags=flags)
    linhas = []

    def ler():
        for linha in p.stderr:
            linhas.append(linha)
    threading.Thread(target=ler, daemon=True).start()
    fim = time.time() + 15
    while time.time() < fim:
        if any("Capturing on" in l for l in linhas):
            time.sleep(1)
            return p
        if p.poll() is not None:
            break
        time.sleep(0.2)
    parar_processo(p)
    raise RuntimeError("a captura não iniciou: " + " ".join(linhas)[:200])


def parar_captura(p):
    if p is None or p.poll() is not None:
        return
    try:
        if os.name == "nt":
            p.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            p.send_signal(signal.SIGINT)
        p.wait(timeout=20)
    except Exception:  # noqa: BLE001
        p.kill()


class Monitor(threading.Thread):
    """Mede a CPU do gerador de carga e dos servidores (100% = 1 núcleo)."""

    def __init__(self, pid_gerador, pids_servidor):
        super().__init__(daemon=True)
        self.pid_gerador, self.pids_servidor = pid_gerador, pids_servidor
        self.parar = threading.Event()
        self.gerador, self.servidor = [], []
        self.cache = {}

    def _cpu(self, pid):
        try:
            p = self.cache.setdefault(pid, psutil.Process(pid))
            total = p.cpu_percent(None)
            for f in p.children(recursive=True):
                total += self.cache.setdefault(f.pid, f).cpu_percent(None)
            return total
        except psutil.Error:
            return 0.0

    def run(self):
        if psutil is None:
            return
        self._cpu(self.pid_gerador)
        for pid in self.pids_servidor:
            self._cpu(pid)
        while not self.parar.wait(2.0):
            self.gerador.append(self._cpu(self.pid_gerador))
            self.servidor.append(sum(self._cpu(p) for p in self.pids_servidor))


def media(v):
    return round(sum(v) / len(v), 1) if v else ""


# ----------------------------------------------------------------------
# Uma rodada
# ----------------------------------------------------------------------
def executar_rodada(ordem, r, args, ferramentas):
    cen, rep, proto = r["cenario_id"], r["repeticao"], r["protocolo"]
    http = proto == "http"
    porta = PORTA_HTTP if http else PORTA_MQTT
    pasta = args.saida
    linha = {"ordem": ordem, "cenario_id": cen, "protocolo": proto,
             "sensores": r["num_sensores"], "repeticao": rep,
             "semente": r["semente"], "inicio": agora()}
    obs, status = [], "ERRO"
    cap = monitor = runner = None
    servidores = []
    arquivar(pasta, cen, rep)
    try:
        if porta_aberta(PORTA_MQTT) or porta_aberta(PORTA_HTTP):
            raise RuntimeError("porta 1883 ou 8000 já está em uso antes da rodada")

        arq_cap = os.path.join(pasta, f"cap_{cen}_rep{rep}.pcapng")
        cap = iniciar_captura(ferramentas["tshark"], ferramentas["interface"], porta, arq_cap)

        if http:
            api = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "servidores.api_http:app",
                 "--port", str(PORTA_HTTP), "--workers", "1",
                 "--timeout-keep-alive", "60", "--no-access-log", "--log-level", "warning"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            servidores.append(api)
            if not esperar_porta(PORTA_HTTP):
                raise RuntimeError("a API não subiu")
            time.sleep(1)
        else:
            broker = subprocess.Popen([ferramentas["mosquitto"], "-c", args.mosquitto_conf],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            servidores.append(broker)
            if not esperar_porta(PORTA_MQTT):
                raise RuntimeError("o broker não subiu")
            qos = "1" if proto.endswith("qos1") else "0"
            sub = subprocess.Popen(
                [sys.executable, "-m", "servidores.subscriber_mqtt",
                 "--qos", qos, "--cenario", cen, "--repeticao", str(rep), "--saida", pasta],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            servidores.append(sub)
            time.sleep(3)
            if sub.poll() is not None:
                raise RuntimeError("o subscriber encerrou logo ao iniciar")

        log = open(os.path.join(pasta, f"log_{cen}_rep{rep}.txt"), "w", encoding="utf-8")
        runner = subprocess.Popen(
            [sys.executable, "-m", "clientes.executar_rodada",
             "--protocolo", proto, "--sensores", str(r["num_sensores"]),
             "--repeticao", str(rep), "--semente", str(r["semente"]),
             "--aquecimento", str(args.aquecimento), "--duracao", str(args.duracao),
             "--tolerancia-fim", str(args.tolerancia_fim), "--saida", pasta],
            stdout=log, stderr=subprocess.STDOUT)
        monitor = Monitor(runner.pid, [p.pid for p in servidores])
        monitor.start()
        limite = args.aquecimento + args.duracao + args.tolerancia_fim + 180
        try:
            rc = runner.wait(timeout=limite)
        except subprocess.TimeoutExpired:
            parar_processo(runner)
            raise RuntimeError("a rodada excedeu o tempo limite")
        finally:
            log.close()
            monitor.parar.set()
            monitor.join(timeout=5)
        if rc != 0:
            raise RuntimeError(f"executar_rodada terminou com código {rc} (veja log_{cen}_rep{rep}.txt)")

        resumo = os.path.join(pasta, f"resumo_servidor_{cen}_rep{rep}.json")
        recebidas = os.path.join(pasta, f"recebidas_{cen}_rep{rep}.csv")
        if not os.path.exists(resumo):
            obs.append("arquivos do servidor ausentes; tentou finalizar manualmente")
            try:
                corpo_fin = json.dumps(
                    {"cenario_id": cen, "repeticao": rep, "diretorio_saida": pasta}
                )
                if http:
                    req_fin = urllib.request.Request(
                        f"http://127.0.0.1:{PORTA_HTTP}/finalizar",
                        data=corpo_fin.encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    urllib.request.urlopen(req_fin, timeout=10)
                else:
                    subprocess.run([ferramentas["mosquitto_pub"], "-h", "localhost", "-t",
                                    "lab/controle/finalizar", "-m", corpo_fin], timeout=10)
            except Exception as e:  # noqa: BLE001
                obs.append(f"falha no finalizar: {e}")
            time.sleep(3)
        if not os.path.exists(resumo):
            raise RuntimeError("os arquivos do servidor não foram gravados")

        time.sleep(2)
        parar_captura(cap)
        if not os.path.exists(arq_cap) or os.path.getsize(arq_cap) == 0:
            raise RuntimeError("o arquivo de captura está ausente ou vazio")

        linha["cpu_gerador_media"] = media(monitor.gerador)
        linha["cpu_gerador_max"] = round(max(monitor.gerador), 1) if monitor.gerador else ""
        linha["cpu_servidor_media"] = media(monitor.servidor)

        status = "OK"
        with open(resumo, encoding="utf-8") as f:
            dados = json.load(f)
        if dados.get("total_invalidas", 0) > 0:
            status = "REVISAR"
            obs.append(f"{dados['total_invalidas']} mensagens inválidas")
        try:
            from analise_piloto import analisar
            res = analisar(recebidas, args.aquecimento, INTERVALO_S)
            linha.update({"lat_media_ms": res["latencia_media_ms"],
                          "lat_p95_ms": res["latencia_p95_ms"],
                          "lat_p99_ms": res["latencia_p99_ms"],
                          "perdas": res["perdas_por_lacuna"],
                          "duplicatas": res["duplicatas"],
                          "agend_max_ms": res["agend_desvio_max_ms"],
                          "agend_acima_50ms": res["agend_acima_50ms"]})
            if res["latencias_negativas"] > 0:
                status = "REVISAR"
                obs.append("latências negativas")
            n_int = max(1, res["leituras_analisadas"] - res["sensores"])
            pct = 100.0 * res["agend_acima_50ms"] / n_int
            if pct > args.agend_tolerancia_pct:
                status = "REVISAR"
                obs.append(f"{res['agend_acima_50ms']} de {n_int} intervalos de envio ({pct:.2f}%) "
                           f"com desvio acima de 50 ms")
            elif res["agend_acima_50ms"] > 0:
                obs.append(f"{res['agend_acima_50ms']} de {n_int} intervalos ({pct:.2f}%) acima de 50 ms, "
                           f"dentro da tolerância")
        except Exception as e:  # noqa: BLE001
            status = "REVISAR"
            obs.append(f"análise rápida falhou: {e}")
        if monitor.gerador and media(monitor.gerador) >= args.cpu_limite:
            status = "REVISAR"
            obs.append(f"CPU média do gerador {media(monitor.gerador)}% (limite {args.cpu_limite}%)")
    except Exception as e:  # noqa: BLE001
        status = "ERRO"
        obs.append(str(e))
    finally:
        if monitor:
            monitor.parar.set()
        if runner and runner.poll() is None:
            parar_processo(runner)
        parar_captura(cap)
        for p in reversed(servidores):
            parar_processo(p)
        esperar_porta(PORTA_MQTT, aberta=False, limite=20)
        esperar_porta(PORTA_HTTP, aberta=False, limite=20)

    if status == "ERRO":
        arquivar(pasta, cen, rep)
    linha.update({"fim": agora(), "status": status, "observacoes": " | ".join(obs)})
    return linha


# ----------------------------------------------------------------------
# Metadados (RN38)
# ----------------------------------------------------------------------
def gravar_metadados(pasta, ferramentas, plano_path, args):
    def versao(pacote):
        try:
            return metadata.version(pacote)
        except metadata.PackageNotFoundError:
            return None

    def saida(cmd):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            return (r.stdout or r.stderr).strip().splitlines()[0]
        except Exception:  # noqa: BLE001
            return None

    meta = {
        "gerado_em": agora(),
        "python": sys.version,
        "sistema": platform.platform(),
        "processador": platform.processor(),
        "nucleos_logicos": os.cpu_count(),
        "pacotes": {p: versao(p) for p in ("fastapi", "uvicorn", "paho-mqtt", "requests",
                                           "pydantic", "pandas", "numpy", "psutil", "dpkt")},
        "mosquitto": saida([ferramentas["mosquitto"], "-h"]),
        "tshark": saida([ferramentas["tshark"], "--version"]),
        "interface_captura": ferramentas["interface"],
        "parametros": {"aquecimento_s": args.aquecimento, "duracao_s": args.duracao,
                       "tolerancia_fim_s": args.tolerancia_fim, "pausa_s": args.pausa},
        "plano": os.path.abspath(plano_path),
    }
    with open(os.path.join(pasta, "coleta_metadados.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)


# ----------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--plano", required=True, help="JSON gerado por --gerar-plano")
    ap.add_argument("--saida", default=os.path.join("dados", "coleta"))
    ap.add_argument("--aquecimento", type=int, default=30)
    ap.add_argument("--duracao", type=int, default=600)
    ap.add_argument("--tolerancia-fim", type=int, default=10)
    ap.add_argument("--pausa", type=int, default=45, help="pausa entre rodadas (s)")
    ap.add_argument("--max-rodadas", type=int, default=0, help="0 = todas as pendentes")
    ap.add_argument("--interface", default="auto", help="número da interface de loopback do tshark")
    ap.add_argument("--tshark", default=None)
    ap.add_argument("--mosquitto", default=None)
    ap.add_argument("--mosquitto-conf", default="mosquitto.conf")
    ap.add_argument("--cpu-limite", type=float, default=80.0,
                    help="CPU média (%% de um núcleo) do gerador acima da qual a rodada é marcada REVISAR")
    ap.add_argument("--agend-tolerancia-pct", type=float, default=1.0,
                    help="%% máxima de intervalos de envio com desvio acima de 50 ms (padrão 1)")
    ap.add_argument("--cargas", type=int, nargs="+", choices=CARGAS,
                    help="só estas quantidades de sensores (ex.: --cargas 10 100)")
    ap.add_argument("--repeticoes", type=int, nargs="+", choices=REPETICOES,
                    help="só estas repetições (ex.: --repeticoes 1)")
    ap.add_argument("--protocolos", nargs="+", choices=PROTOCOLOS,
                    help="só estes protocolos")
    ap.add_argument("--repetir-revisar", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="só mostra o que seria executado")
    args = ap.parse_args()

    plano = carregar_plano(args.plano)
    os.makedirs(args.saida, exist_ok=True)
    arq_prog = os.path.join(args.saida, "coleta_progresso.csv")
    fila = pendentes(plano, ler_progresso(arq_prog), args.repetir_revisar)
    fila = [(o, r) for o, r in fila
            if (not args.cargas or r["num_sensores"] in args.cargas)
            and (not args.repeticoes or r["repeticao"] in args.repeticoes)
            and (not args.protocolos or r["protocolo"] in args.protocolos)]
    if args.max_rodadas:
        fila = fila[:args.max_rodadas]

    por_rodada = args.aquecimento + args.duracao + args.tolerancia_fim + 60 + args.pausa
    print(f"Plano válido: 45 rodadas. Pendentes nesta execução: {len(fila)}")
    print(f"Tempo estimado: {len(fila) * por_rodada / 3600:.1f} h "
          f"({por_rodada / 60:.1f} min por rodada)")
    if args.dry_run or not fila:
        for ordem, r in fila:
            print(f"  #{ordem:02d} {r['cenario_id']:<16} rep {r['repeticao']} semente {r['semente']}")
        return

    ferramentas = {
        "tshark": args.tshark or achar_exe("tshark", [r"C:\Program Files\Wireshark\tshark.exe"]),
        "mosquitto": args.mosquitto or achar_exe("mosquitto", [r"C:\Program Files\mosquitto\mosquitto.exe"]),
        "mosquitto_pub": achar_exe("mosquitto_pub", [r"C:\Program Files\mosquitto\mosquitto_pub.exe"]),
    }
    faltando = [k for k, v in ferramentas.items() if not v]
    if faltando:
        sys.exit(f"Ferramentas não encontradas: {faltando}. Confira o PATH.")
    if not os.path.exists(args.mosquitto_conf):
        sys.exit(f"{args.mosquitto_conf} não encontrado. Rode na raiz do projeto.")
    ferramentas["interface"] = (detectar_interface(ferramentas["tshark"])
                                if args.interface == "auto" else args.interface)
    if not ferramentas["interface"]:
        sys.exit("Interface de loopback não encontrada (tshark -D). Reinstale o Npcap com suporte a loopback.")
    if psutil is None:
        print("AVISO: psutil não instalado; a CPU não será monitorada (pip install psutil).")
    if porta_aberta(PORTA_MQTT) or porta_aberta(PORTA_HTTP):
        sys.exit("A porta 1883 ou 8000 já está em uso. Feche o broker/API/serviço do Mosquitto.")

    gravar_metadados(args.saida, ferramentas, args.plano, args)
    print(f"Interface de captura: {ferramentas['interface']}. Não use o computador durante a coleta.\n")

    try:
        for i, (ordem, r) in enumerate(fila, 1):
            print(f"[{i}/{len(fila)}] #{ordem:02d} {r['cenario_id']} rep {r['repeticao']} "
                  f"(início {datetime.now():%H:%M:%S})...", flush=True)
            linha = executar_rodada(ordem, r, args, ferramentas)
            gravar_progresso(arq_prog, linha)
            print(f"    -> {linha['status']}  lat média {linha.get('lat_media_ms', '-')} ms, "
                  f"p99 {linha.get('lat_p99_ms', '-')} ms, CPU gerador {linha.get('cpu_gerador_media', '-')}%"
                  + (f"  [{linha['observacoes']}]" if linha["observacoes"] else ""), flush=True)
            if i < len(fila):
                time.sleep(args.pausa)
    except KeyboardInterrupt:
        print("\nInterrompido. Rode o mesmo comando para retomar de onde parou.")
        sys.exit(130)

    ult = ler_progresso(arq_prog)
    cont = {}
    for linha in ult.values():
        cont[linha["status"]] = cont.get(linha["status"], 0) + 1
    print("\nSituação geral:", cont)


if __name__ == "__main__":
    main()