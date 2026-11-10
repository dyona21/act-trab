"""
Orquestrador de rodadas de experimento (RN02, RN03, RN18, RN31 a RN39).

Responsabilidades:
- Instanciar N sensores (10, 50 ou 100) com conexões persistentes exclusivas (RN29)
- Agendar envios a cada 5 s sem deriva: t_alvo = inicio + defasagem + n * 5 s (RN02, RN03)
- Medir o atraso de agendamento em ms para verificar o limite de 50 ms com 100 sensores (RN39)
- Monitorar o uso de CPU do gerador de carga para verificar o limite de 80% (RN37)
- Aguardar a janela final e solicitar a gravação em disco no servidor (POST /finalizar ou MQTT controle)
- Salvar CSV de leituras enviadas, cronograma de excursões (RN18) e metadados da rodada (RN38)
"""

import argparse
import csv
import datetime as dt
import importlib.metadata
import json
import os
import platform
import random
import sys
import threading
import time
from pathlib import Path
from typing import Any
import paho.mqtt.client as mqtt
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from clientes.cliente_http import ClienteHTTPSensor
from clientes.cliente_mqtt import ClienteMQTTSensor
from clientes.sensor import (
    SimuladorSensorDHT22,
    gerar_defasagens_e_excursoes,
    salvar_cronograma_excursoes,
)
from comum.modelo import (
    ATRASO_MAX_AGENDAMENTO_MS,
    DURACAO_MEDICAO_S,
    INTERVALO_ENVIO_S,
    LIMITE_CPU_GERADOR_PCT,
    TEMPO_AQUECIMENTO_S,
    TOLERANCIA_FIM_S,
)
from servidores.subscriber_mqtt import TOPICO_CONTROLE_FINALIZAR


def obter_versoes_ambiente() -> dict[str, str]:
    """Coleta versões do Python e das bibliotecas instaladas para os metadados da rodada (RN38)."""
    pacotes = ["fastapi", "uvicorn", "paho-mqtt", "requests", "pandas", "matplotlib", "pydantic"]
    versoes = {"python": platform.python_version(), "plataforma": platform.platform()}
    for pkg in pacotes:
        try:
            versoes[pkg] = importlib.metadata.version(pkg)
        except importlib.metadata.PackageNotFoundError:
            versoes[pkg] = "nao_instalado"
    return versoes


def executar_loop_sensor(
    simulador: SimuladorSensorDHT22,
    cliente: ClienteHTTPSensor | ClienteMQTTSensor,
    t0_perf_ns: int,
    aquecimento_s: float,
    duracao_total_s: float,
    buffer_saida: list[tuple[Any, ...]],
    atrasos_agendamento_ms: list[float],
    evento_parada: threading.Event,
) -> None:
    """
    Thread de envio de um sensor individual seguindo RN02:
    O envio n ocorre exatamente em t0_perf_ns + defasagem_inicial + n * 5.0 s, sem deriva cumulativa.
    """
    n = 0
    defasagem = simulador.defasagem_inicial_s

    while not evento_parada.is_set():
        tempo_relativo_s = defasagem + n * INTERVALO_ENVIO_S
        if tempo_relativo_s >= duracao_total_s:
            break

        t_alvo_ns = t0_perf_ns + int(tempo_relativo_s * 1_000_000_000)
        restante_s = (t_alvo_ns - time.perf_counter_ns()) / 1_000_000_000.0
        if restante_s > 0:
            # Dorme até o instante exato do disparo
            if evento_parada.wait(timeout=restante_s):
                break

        instante_disparo_ns = time.perf_counter_ns()
        atraso_ms = max(0.0, (instante_disparo_ns - t_alvo_ns) / 1_000_000.0)
        atrasos_agendamento_ms.append(atraso_ms)

        dados, payload_bytes, excursao_id = simulador.gerar_leitura(tempo_relativo_s)
        sucesso_envio, status_envio = cliente.enviar_leitura(payload_bytes)
        em_aquecimento = tempo_relativo_s < aquecimento_s

        buffer_saida.append(
            (
                dados["sensor_id"],
                dados["seq"],
                dados["t_envio"],
                dados["temperatura"],
                dados["umidade"],
                len(payload_bytes),
                round(tempo_relativo_s, 4),
                em_aquecimento,
                excursao_id if excursao_id is not None else "",
                sucesso_envio,
                status_envio,
                round(atraso_ms, 3),
            )
        )
        n += 1


def finalizar_servidor_http(
    url_base: str,
    cenario_id: str,
    repeticao: int,
    diretorio_saida: str,
) -> dict[str, Any]:
    """Chama POST /finalizar na API HTTP para descarregar os buffers em disco (RN25, RN26)."""
    url = f"{url_base.rstrip('/')}/finalizar"
    resp = requests.post(
        url,
        json={
            "cenario_id": cenario_id,
            "repeticao": repeticao,
            "diretorio_saida": diretorio_saida,
            "limpar_memoria": True,
        },
        timeout=10.0,
    )
    resp.raise_for_status()
    return resp.json()


def finalizar_servidor_mqtt(
    broker_host: str,
    broker_port: int,
    cenario_id: str,
    repeticao: int,
    diretorio_saida: str,
    encerrar_subscriber: bool = False,
) -> None:
    """Publica comando no tópico de controle para o subscriber gravar seus buffers em disco (RN26)."""
    c = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"controle_finalizar_{cenario_id}_{repeticao}",
    )
    c.connect(broker_host, broker_port, keepalive=30)
    c.loop_start()
    payload = json.dumps(
        {
            "cenario_id": cenario_id,
            "repeticao": repeticao,
            "diretorio_saida": diretorio_saida,
            "encerrar": encerrar_subscriber,
        }
    ).encode("utf-8")
    info = c.publish(TOPICO_CONTROLE_FINALIZAR, payload=payload, qos=1)
    info.wait_for_publish(timeout=5.0)
    time.sleep(0.5)
    c.loop_stop()
    c.disconnect()


def executar_rodada(
    protocolo: str,
    num_sensores: int,
    repeticao: int,
    semente: int,
    aquecimento_s: float = TEMPO_AQUECIMENTO_S,
    medicao_s: float = DURACAO_MEDICAO_S,
    tolerancia_fim_s: float = TOLERANCIA_FIM_S,
    url_http: str = "http://172.20.10.10:8000",
    broker_host: str = "172.20.10.10",
    broker_port: int = 1883,
    diretorio_saida: str = "dados",
    arquivo_captura: str = "",
) -> dict[str, Any]:
    """
    Executa uma rodada completa de experimento (aquecimento + medição + tolerância de 10 s).
    protocolo: 'http', 'mqtt_qos0' ou 'mqtt_qos1'
    """
    protocolo_norm = protocolo.lower()
    if protocolo_norm not in ("http", "mqtt_qos0", "mqtt_qos1"):
        raise ValueError("protocolo deve ser 'http', 'mqtt_qos0' ou 'mqtt_qos1'")

    cenario_id = f"{protocolo_norm}_{num_sensores}s"
    pasta_saida = Path(diretorio_saida)
    pasta_saida.mkdir(parents=True, exist_ok=True)
    duracao_total_s = aquecimento_s + medicao_s

    # 1. Gera defasagens (RN03) e cronograma de excursões (RN16-RN19) determinísticos pela semente
    defasagens, excursoes = gerar_defasagens_e_excursoes(
        num_sensores=num_sensores,
        semente=semente,
        duracao_total_s=duracao_total_s,
        aquecimento_s=aquecimento_s,
    )
    arq_excursoes = pasta_saida / f"excursoes_{cenario_id}_rep{repeticao}.csv"
    salvar_cronograma_excursoes(excursoes, arq_excursoes)

    excursoes_por_sensor: dict[str, list] = {s: [] for s in defasagens}
    for exc in excursoes:
        excursoes_por_sensor[exc.sensor_id].append(exc)

    # 2. Instancia simuladores e conexões persistentes (1 por sensor - RN29)
    simuladores: list[SimuladorSensorDHT22] = []
    clientes: list[ClienteHTTPSensor | ClienteMQTTSensor] = []

    for sensor_id, defasagem_s in defasagens.items():
        sim = SimuladorSensorDHT22(
            sensor_id=sensor_id,
            semente_base=semente,
            defasagem_inicial_s=defasagem_s,
            excursoes_do_sensor=excursoes_por_sensor[sensor_id],
        )
        simuladores.append(sim)

        if protocolo_norm == "http":
            cli: ClienteHTTPSensor | ClienteMQTTSensor = ClienteHTTPSensor(
                sensor_id=sensor_id, url_base=url_http
            )
        else:
            qos = 1 if protocolo_norm == "mqtt_qos1" else 0
            cli = ClienteMQTTSensor(
                sensor_id=sensor_id,
                broker_host=broker_host,
                broker_port=broker_port,
                qos=qos,
            )
        cli.conectar()
        clientes.append(cli)

    # 3. Prepara estruturas em memória (RN26) e monitoramento de CPU (RN37)
    buffers_por_sensor: list[list[tuple[Any, ...]]] = [[] for _ in range(num_sensores)]
    atrasos_por_sensor: list[list[float]] = [[] for _ in range(num_sensores)]
    evento_parada = threading.Event()

    num_cpus = max(1, os.cpu_count() or 1)
    cpu_proc_inicio = time.process_time()
    t0_perf_ns = time.time_ns()
    horario_inicio_iso = dt.datetime.now(dt.timezone.utc).isoformat()

    threads: list[threading.Thread] = []
    for i in range(num_sensores):
        t = threading.Thread(
            target=executar_loop_sensor,
            args=(
                simuladores[i],
                clientes[i],
                t0_perf_ns,
                aquecimento_s,
                duracao_total_s,
                buffers_por_sensor[i],
                atrasos_por_sensor[i],
                evento_parada,
            ),
            daemon=True,
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    t_fim_envios_ns = time.time_ns()
    duracao_real_perf = max(0.001, (t_fim_envios_ns - t0_perf_ns) / 1_000_000_000.0)
    cpu_proc_fim = time.process_time()
    uso_cpu_medio_pct = round(((cpu_proc_fim - cpu_proc_inicio) / (duracao_real_perf * num_cpus)) * 100.0, 2)

    # RN31: Aguarda até 10 s após o fim dos envios para mensagens em trânsito chegarem
    if tolerancia_fim_s > 0:
        time.sleep(tolerancia_fim_s)
    t_limite_recepcao_ns = t_fim_envios_ns + int(tolerancia_fim_s * 1_000_000_000)

    # 4. Fecha conexões dos clientes e aciona gravação no servidor (RN25, RN26)
    for cli in clientes:
        cli.fechar()

    erro_servidor = None
    try:
        if protocolo_norm == "http":
            finalizar_servidor_http(url_http, cenario_id, repeticao, diretorio_saida)
        else:
            finalizar_servidor_mqtt(broker_host, broker_port, cenario_id, repeticao, diretorio_saida)
    except Exception as exc:
        erro_servidor = str(exc)

    # 5. Grava CSV de leituras enviadas pelo gerador de carga
    todas_enviadas: list[tuple[Any, ...]] = []
    for b in buffers_por_sensor:
        todas_enviadas.extend(b)
    todas_enviadas.sort(key=lambda x: (x[2], x[0], x[1]))

    arq_enviadas = pasta_saida / f"enviadas_{cenario_id}_rep{repeticao}.csv"
    with arq_enviadas.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "sensor_id",
                "seq",
                "t_envio",
                "temperatura",
                "umidade",
                "bytes_payload",
                "tempo_relativo_s",
                "em_aquecimento",
                "excursao_id",
                "sucesso_envio",
                "status_envio",
                "atraso_agendamento_ms",
            ]
        )
        writer.writerows(todas_enviadas)

    # 6. Avalia validade da rodada conforme RN37 e RN39
    todos_atrasos = [a for lista in atrasos_por_sensor for a in lista]
    atraso_max_ms = round(max(todos_atrasos), 3) if todos_atrasos else 0.0
    atraso_medio_ms = round(sum(todos_atrasos) / len(todos_atrasos), 3) if todos_atrasos else 0.0

    motivos_invalidacao: list[str] = []
    if erro_servidor:
        motivos_invalidacao.append(f"RN37: Erro ao finalizar servidor ({erro_servidor})")
    if uso_cpu_medio_pct > LIMITE_CPU_GERADOR_PCT:
        motivos_invalidacao.append(
            f"RN37: CPU do gerador ({uso_cpu_medio_pct}%) ultrapassou {LIMITE_CPU_GERADOR_PCT}%"
        )
    if num_sensores == 100 and atraso_max_ms > ATRASO_MAX_AGENDAMENTO_MS:
        motivos_invalidacao.append(
            f"RN39: Atraso máximo de agendamento ({atraso_max_ms} ms) ultrapassou {ATRASO_MAX_AGENDAMENTO_MS} ms"
        )

    # RN38: Salva metadados completos da rodada
    t_fim_aquecimento_ns = t0_perf_ns + int(aquecimento_s * 1_000_000_000)
    metadados = {
        "cenario_id": cenario_id,
        "protocolo": protocolo_norm,
        "num_sensores": num_sensores,
        "repeticao": repeticao,
        "semente": semente,
        "horario_inicio_utc": horario_inicio_iso,
        "t_inicio_rodada_ns": t0_perf_ns,
        "t_fim_aquecimento_ns": t_fim_aquecimento_ns,
        "t_fim_envios_ns": t_fim_envios_ns,
        "t_limite_recepcao_ns": t_limite_recepcao_ns,
        "aquecimento_s": aquecimento_s,
        "medicao_s": medicao_s,
        "tolerancia_fim_s": tolerancia_fim_s,
        "uso_cpu_gerador_pct": uso_cpu_medio_pct,
        "atraso_agendamento_medio_ms": atraso_medio_ms,
        "atraso_agendamento_max_ms": atraso_max_ms,
        "rodada_valida": len(motivos_invalidacao) == 0,
        "motivos_invalidacao": motivos_invalidacao,
        "arquivo_captura_rede": arquivo_captura or f"dados/captura_{cenario_id}_rep{repeticao}.pcapng",
        "arquivo_enviadas": str(arq_enviadas),
        "arquivo_excursoes": str(arq_excursoes),
        "versoes": obter_versoes_ambiente(),
    }

    arq_meta = pasta_saida / f"metadados_{cenario_id}_rep{repeticao}.json"
    with arq_meta.open("w", encoding="utf-8") as f:
        json.dump(metadados, f, indent=2, ensure_ascii=False)

    return metadados


def gerar_ordem_sorteada_rodadas(semente_experimento: int = 2026) -> list[dict[str, Any]]:
    """
    Gera as 45 rodadas do experimento (9 cenários x 5 repetições - RN32, RN33)
    em ordem sorteada (RN35), garantindo que a mesma repetição compartilhe a mesma
    semente de sensores/excursões nos três protocolos (RN03, RN18).
    """
    protocolos = ["mqtt_qos0", "mqtt_qos1", "http"]
    quantidades_sensores = [10, 50, 100]
    repeticoes = [1, 2, 3, 4, 5]

    # Semente fixa por repetição para que defasagens (RN03) e excursões (RN18) sejam idênticas nos 3 protocolos
    sementes_por_rep = {rep: 10000 + rep * 137 for rep in repeticoes}

    rodadas: list[dict[str, Any]] = []
    for rep in repeticoes:
        for n_sens in quantidades_sensores:
            for proto in protocolos:
                rodadas.append(
                    {
                        "protocolo": proto,
                        "num_sensores": n_sens,
                        "repeticao": rep,
                        "semente": sementes_por_rep[rep],
                        "cenario_id": f"{proto}_{n_sens}s",
                    }
                )

    rng = random.Random(semente_experimento)
    rng.shuffle(rodadas)
    return rodadas


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestrador de rodada de teste (HTTP vs MQTT)")
    parser.add_argument(
        "--protocolo",
        choices=["http", "mqtt_qos0", "mqtt_qos1"],
        default="http",
        help="Protocolo do cenário (RN32)",
    )
    parser.add_argument(
        "--sensores",
        type=int,
        choices=[10, 50, 100],
        default=10,
        help="Quantidade de sensores simulados: 10, 50 ou 100 (RN32)",
    )
    parser.add_argument("--repeticao", type=int, default=1, help="Número da repetição (1 a 5)")
    parser.add_argument("--semente", type=int, default=10137, help="Semente aleatória da repetição")
    parser.add_argument(
        "--aquecimento",
        type=float,
        default=TEMPO_AQUECIMENTO_S,
        help="Tempo de aquecimento em segundos (padrão RN33: 30)",
    )
    parser.add_argument(
        "--duracao",
        type=float,
        default=DURACAO_MEDICAO_S,
        help="Duração da medição em segundos após aquecimento (padrão RN33: 600)",
    )
    parser.add_argument(
        "--tolerancia-fim",
        type=float,
        default=TOLERANCIA_FIM_S,
        help="Tolerância após o término em segundos (padrão RN31: 10)",
    )
    parser.add_argument("--url-http", default="http://127.0.0.1:8000", help="URL base da API HTTP")
    parser.add_argument("--broker-host", default="127.0.0.1", help="Host do broker Mosquitto")
    parser.add_argument("--broker-port", type=int, default=1883, help="Porta do broker Mosquitto")
    parser.add_argument("--saida", default="dados", help="Diretório de saída dos arquivos")
    parser.add_argument(
        "--gerar-plano",
        action="store_true",
        help="Apenas gera e salva o plano sorteado das 45 rodadas (RN32, RN35)",
    )

    args = parser.parse_args()

    if args.gerar_plano:
        plano = gerar_ordem_sorteada_rodadas()
        for nome_arq in ("plano_rodadas.json", "plano_sorteado_45_rodadas.json"):
            arq_plano = Path(args.saida) / nome_arq
            arq_plano.parent.mkdir(parents=True, exist_ok=True)
            with arq_plano.open("w", encoding="utf-8") as f:
                json.dump(plano, f, indent=2, ensure_ascii=False)
            print(f"Plano sorteado com {len(plano)} rodadas salvo em: {arq_plano}")
    else:
        meta = executar_rodada(
            protocolo=args.protocolo,
            num_sensores=args.sensores,
            repeticao=args.repeticao,
            semente=args.semente,
            aquecimento_s=args.aquecimento,
            medicao_s=args.duracao,
            tolerancia_fim_s=args.tolerancia_fim,
            url_http=args.url_http,
            broker_host=args.broker_host,
            broker_port=args.broker_port,
            diretorio_saida=args.saida,
        )
        print(json.dumps(meta, indent=2, ensure_ascii=False))

