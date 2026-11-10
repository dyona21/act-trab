"""Análise Final Completa dos Dados Coletados (HTTP vs. MQTT QoS 0 vs. MQTT QoS 1).

Lê automaticamente todos os artefatos da pasta de coleta (ex.: dados/coleta_final):
  - recebidas_*.csv e enviadas_*.csv (latência, entrega exata, perdas e duplicatas)
  - alertas_*.csv e excursoes_*.csv (tempo de detecção de anomalias e p99 de alerta)
  - cap_*.pcapng (parser binário nativo de pacotes e bytes reais na rede)
  - metadados_*.json, resumo_servidor_*.json e coleta_progresso.csv (CPU e contadores)

Uso (na raiz do projeto, com .venv ativo):
    python analise_final.py --diretorio dados/coleta_final
"""

import argparse
import glob
import json
import math
import os
import re
import struct
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def ler_pcapng_nativo(caminho_pcapng: str) -> dict:
    """Lê um arquivo .pcapng nativamente em Python (sem precisar de Wireshark/Scapy)
    e soma o total de pacotes capturados e o total de bytes trafegados na rede.
    """
    if not os.path.exists(caminho_pcapng):
        return {
            "pacotes_rede": 0,
            "bytes_rede_total": 0,
            "tamanho_arquivo_pcap_bytes": 0
        }

    tamanho_arq = os.path.getsize(caminho_pcapng)
    pacotes = 0
    bytes_capturados = 0
    bytes_no_fio = 0

    try:
        with open(caminho_pcapng, "rb") as f:
            dados = f.read()

        pos = 0
        total_len = len(dados)
        endian = "<"  # padrão little-endian na maioria das máquinas x86_64

        while pos + 8 <= total_len:
            block_type, block_total_length = struct.unpack_from(
                endian + "II", dados, pos
            )

            # Section Header Block (0x0A0D0D0A) define o Byte-Order Magic
            if block_type == 0x0A0D0D0A and pos + 12 <= total_len:
                magic = struct.unpack_from("<I", dados, pos + 8)[0]
                if magic == 0x1A2B3C4D:
                    endian = "<"
                elif magic == 0x4D3C2B1A:
                    endian = ">"
                block_total_length = struct.unpack_from(
                    endian + "I", dados, pos + 4
                )[0]

            if block_total_length < 12 or pos + block_total_length > total_len:
                break

            # Enhanced Packet Block (EPB = 0x00000006)
            if block_type == 0x00000006 and block_total_length >= 32:
                # Campos do EPB: InterfaceID(4), TimestampHigh(4), TimestampLow(4), CapturedLen(4), OriginalLen(4)
                cap_len, orig_len = struct.unpack_from(
                    endian + "II", dados, pos + 20
                )
                pacotes += 1
                bytes_capturados += cap_len
                bytes_no_fio += orig_len

            # Simple Packet Block (SPB = 0x00000003)
            elif block_type == 0x00000003 and block_total_length >= 16:
                orig_len = struct.unpack_from(endian + "I", dados, pos + 8)[0]
                pacotes += 1
                bytes_capturados += orig_len
                bytes_no_fio += orig_len

            pos += block_total_length

    except Exception as e:  # noqa: BLE001
        print(
            f"[Aviso] Falha ao parsear blocos de {os.path.basename(caminho_pcapng)}: {e}",
            file=sys.stderr
        )

    return {
        "pacotes_rede": pacotes,
        "bytes_rede_total": bytes_no_fio if bytes_no_fio > 0 else tamanho_arq,
        "tamanho_arquivo_pcap_bytes": tamanho_arq
    }


def carregar_json_seguro(caminho: str) -> dict:
    if os.path.exists(caminho):
        try:
            with open(caminho, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:  # noqa: BLE001
            return {}
    return {}


def extrair_info_sufixo(sufixo: str) -> tuple[str, int, int]:
    """Extrai (protocolo, n_sensores, repeticao) de ex.: 'mqtt_qos1_100s_rep2'."""
    m = re.match(r"^(http|mqtt_qos0|mqtt_qos1)_(\d+)s_rep(\d+)$", sufixo)
    if m:
        return m.group(1), int(m.group(2)), int(m.group(3))
    return sufixo, 0, 0


def analisar_rodada(
    diretorio: str,
    caminho_recebidas: str,
    aquecimento: float,
    intervalo: float,
    df_progresso: pd.DataFrame | None
) -> dict:
    nome_arq = os.path.basename(caminho_recebidas)
    sufixo = nome_arq.replace("recebidas_", "").replace(".csv", "")
    proto_nome, sensores_nome, rep_nome = extrair_info_sufixo(sufixo)

    # Caminhos dos demais arquivos da mesma rodada
    caminho_enviadas = os.path.join(diretorio, f"enviadas_{sufixo}.csv")
    caminho_alertas = os.path.join(diretorio, f"alertas_{sufixo}.csv")
    caminho_excursoes = os.path.join(diretorio, f"excursoes_{sufixo}.csv")
    caminho_pcap = os.path.join(diretorio, f"cap_{sufixo}.pcapng")
    caminho_meta = os.path.join(diretorio, f"metadados_{sufixo}.json")
    caminho_resumo_srv = os.path.join(
        diretorio, f"resumo_servidor_{sufixo}.json"
    )

    df_rec = pd.read_csv(caminho_recebidas)
    protocolo = (
        str(df_rec["protocolo"].iloc[0])
        if "protocolo" in df_rec.columns and len(df_rec)
        else proto_nome
    )
    n_desc = math.ceil(aquecimento / intervalo)

    # Filtra janela de medição (exclui aquecimento seq < n_desc)
    rec_janela = df_rec[df_rec["seq"] >= n_desc].copy()
    duplicatas = (
        int(rec_janela["duplicata"].sum())
        if "duplicata" in rec_janela.columns
        else 0
    )
    unicas = (
        rec_janela[~rec_janela["duplicata"]].copy()
        if "duplicata" in rec_janela.columns
        else rec_janela.copy()
    )

    # Latência em milissegundos
    lat_ms = (unicas["t_recebimento"] - unicas["t_envio"]) / 1e6

    # 1. Cálculo de Perdas por Lacuna (método do piloto)
    esperadas_lacuna = 0
    recebidas_lacuna = 0
    for _, g in unicas.groupby("sensor_id"):
        seqs = set(g["seq"])
        faixa = range(n_desc, int(g["seq"].max()) + 1)
        esperadas_lacuna += len(faixa)
        recebidas_lacuna += len(seqs & set(faixa))
    perdas_lacuna = esperadas_lacuna - recebidas_lacuna

    # 2. Cálculo Exato cruzando com enviadas_*.csv (se existir)
    enviadas_validas = esperadas_lacuna
    if os.path.exists(caminho_enviadas):
        df_env = pd.read_csv(caminho_enviadas)
        if "seq" in df_env.columns:
            df_env_janela = df_env[df_env["seq"] >= n_desc]
            # Se houver coluna indicando sucesso/envio, contamos todos os disparos na janela
            pares_enviados = set(
                zip(df_env_janela["sensor_id"], df_env_janela["seq"])
            )
            pares_recebidos = set(zip(unicas["sensor_id"], unicas["seq"]))
            enviadas_validas = len(pares_enviados)
            entregues_exatas = len(pares_enviados & pares_recebidos)
            perdas_exatas = enviadas_validas - entregues_exatas
        else:
            entregues_exatas = len(unicas)
            perdas_exatas = perdas_lacuna
    else:
        entregues_exatas = len(unicas)
        perdas_exatas = perdas_lacuna

    taxa_entrega_pct = (
        (entregues_exatas / enviadas_validas * 100.0)
        if enviadas_validas > 0
        else 0.0
    )

    # 3. Precisão do agendamento dos sensores (desvio do intervalo de 5 s)
    desvios = []
    for _, g in unicas.sort_values("seq").groupby("sensor_id"):
        t = g["t_envio"].to_numpy()
        consecutivas = np.diff(g["seq"].to_numpy()) == 1
        d = np.diff(t) / 1e6 - intervalo * 1000
        desvios.extend(np.abs(d[consecutivas]))
    desvios = np.array(desvios) if desvios else np.array([0.0])

    # 4. Análise de Alertas e Excursões
    n_alertas = 0
    alerta_medio_ms = np.nan
    alerta_p95_ms = np.nan
    alerta_p99_ms = np.nan
    alerta_max_ms = np.nan
    if os.path.exists(caminho_alertas):
        df_al = pd.read_csv(caminho_alertas)
        if len(df_al) > 0 and "tempo_alerta_ns" in df_al.columns:
            # Se tiver t_abertura ou seq, podemos considerar todos os alertas da medição
            al_ms = df_al["tempo_alerta_ns"] / 1e6
            n_alertas = len(al_ms)
            alerta_medio_ms = round(float(al_ms.mean()), 3)
            alerta_p95_ms = round(float(np.percentile(al_ms, 95)), 3)
            alerta_p99_ms = round(float(np.percentile(al_ms, 99)), 3)
            alerta_max_ms = round(float(al_ms.max()), 3)

    n_excursoes = 0
    if os.path.exists(caminho_excursoes):
        df_exc = pd.read_csv(caminho_excursoes)
        n_excursoes = len(df_exc)

    # 5. Tráfego de Rede via .pcapng
    info_pcap = ler_pcapng_nativo(caminho_pcap)
    bytes_totais = info_pcap["bytes_rede_total"]
    pacotes_totais = info_pcap["pacotes_rede"]
    # Usa total de leituras recebidas (incluindo aquecimento) como divisor do pcap completo
    total_leituras_pcap = max(len(df_rec), 1)
    bytes_por_leitura = round(bytes_totais / total_leituras_pcap, 2)
    pacotes_por_leitura = round(pacotes_totais / total_leituras_pcap, 2)

    # 6. Metadados, Resumo do Servidor e CPU (coleta_progresso.csv)
    meta = carregar_json_seguro(caminho_meta)
    resumo_srv = carregar_json_seguro(caminho_resumo_srv)
    cpu_gerador_pct = np.nan

    # Procura CPU nos metadados ou em coleta_progresso.csv
    for chave_cpu in ("uso_cpu_gerador_pct", "cpu_gerador_pct", "cpu_pct", "cpu_media_pct", "cpu"):
        if chave_cpu in meta and meta[chave_cpu] is not None:
            cpu_gerador_pct = float(meta[chave_cpu])
            break

    if np.isnan(cpu_gerador_pct) and df_progresso is not None:
        # Tenta cruzar pelo cenario_id/cenario e repeticao
        cenario_alvo = f"{proto_nome}_{sensores_nome}s"
        cols = set(df_progresso.columns)
        col_cen = "cenario_id" if "cenario_id" in cols else ("cenario" if "cenario" in cols else None)
        if col_cen and "repeticao" in cols:
            filtro = df_progresso[
                (df_progresso[col_cen] == cenario_alvo)
                & (df_progresso["repeticao"] == rep_nome)
            ]
            for col_c in filtro.columns:
                if "cpu" in col_c.lower() and len(filtro) > 0:
                    val = filtro[col_c].iloc[-1]
                    if pd.notna(val):
                        cpu_gerador_pct = float(val)
                        break

    invalidas_srv = resumo_srv.get("total_invalidas", 0)

    return {
        "cenario_id": f"{proto_nome}_{sensores_nome}s",
        "protocolo": proto_nome,
        "sensores": (
            sensores_nome
            if sensores_nome > 0
            else int(unicas["sensor_id"].nunique())
        ),
        "repeticao": rep_nome,
        "enviadas_janela": int(enviadas_validas),
        "recebidas_unicas": int(entregues_exatas),
        "taxa_entrega_pct": round(taxa_entrega_pct, 4),
        "perdas_exatas": int(perdas_exatas),
        "perdas_por_lacuna": int(perdas_lacuna),
        "duplicatas": int(duplicatas),
        "invalidas_servidor": int(invalidas_srv),
        "latencia_media_ms": round(float(lat_ms.mean()), 3),
        "latencia_dp_ms": round(float(lat_ms.std()), 3),
        "latencia_mediana_ms": round(float(lat_ms.median()), 3),
        "latencia_p95_ms": round(float(np.percentile(lat_ms, 95)), 3),
        "latencia_p99_ms": round(float(np.percentile(lat_ms, 99)), 3),
        "latencia_max_ms": round(float(lat_ms.max()), 3),
        "latencias_negativas": int((lat_ms < 0).sum()),
        "alertas_disparados": int(n_alertas),
        "excursoes_planejadas": int(n_excursoes),
        "alerta_medio_ms": alerta_medio_ms,
        "alerta_p95_ms": alerta_p95_ms,
        "alerta_p99_ms": alerta_p99_ms,
        "alerta_max_ms": alerta_max_ms,
        "pacotes_rede": int(pacotes_totais),
        "bytes_rede_total": int(bytes_totais),
        "bytes_por_mensagem": bytes_por_leitura,
        "pacotes_por_mensagem": pacotes_por_leitura,
        "cpu_gerador_pct": (
            round(cpu_gerador_pct, 2)
            if not np.isnan(cpu_gerador_pct)
            else np.nan
        ),
        "agend_desvio_medio_ms": round(float(desvios.mean()), 2),
        "agend_desvio_max_ms": round(float(desvios.max()), 2),
        "agend_acima_50ms": int((desvios > 50).sum())
    }


def gerar_graficos(df_agregado: pd.DataFrame, diretorio_saida: str):
    """Gera gráficos comparativos em alta resolução (300 DPI) para inclusão no artigo."""
    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")

    nomes_legenda = {
        "http": "HTTP/REST (FastAPI)",
        "mqtt_qos0": "MQTT QoS 0 (Mosquitto)",
        "mqtt_qos1": "MQTT QoS 1 (Mosquitto)"
    }
    cores = {
        "http": "#d95f02",
        "mqtt_qos0": "#1b9e77",
        "mqtt_qos1": "#7570b3"
    }

    protocolos = [p for p in ["mqtt_qos0", "mqtt_qos1", "http"] if p in df_agregado["protocolo"].unique()]
    cargas = sorted(df_agregado["sensores"].unique())
    x = np.arange(len(cargas))
    largura = 0.25

    # 1. Gráfico de Latência (Média, p95 e p99)
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    for i, proto in enumerate(protocolos):
        sub = df_agregado[df_agregado["protocolo"] == proto].sort_values("sensores")
        pos = x + (i - len(protocolos) / 2 + 0.5) * largura
        b1 = axes[0].bar(
            pos,
            sub["latencia_media_ms_mean"],
            largura,
            label=nomes_legenda.get(proto, proto),
            color=cores.get(proto)
        )
        axes[0].bar_label(b1, fmt="%.2f", padding=3, fontsize=8)

        b2 = axes[1].bar(
            pos,
            sub["latencia_p99_ms_mean"],
            largura,
            label=nomes_legenda.get(proto, proto),
            color=cores.get(proto)
        )
        axes[1].bar_label(b2, fmt="%.2f", padding=3, fontsize=8)

    axes[0].set_title("Latência Média por Carga de Sensores")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels([f"{c} sensores" for c in cargas])
    axes[0].set_ylabel("Latência Média (ms)")
    axes[0].legend()

    axes[1].set_title("Latência de Cauda (p99) por Carga de Sensores")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels([f"{c} sensores" for c in cargas])
    axes[1].set_ylabel("Latência p99 (ms)")
    axes[1].legend()

    plt.tight_layout()
    caminho_g1 = os.path.join(diretorio_saida, "grafico_comparativo_latencias.png")
    plt.savefig(caminho_g1, dpi=300)
    plt.close()

    # 2. Gráfico de Tráfego de Rede (Bytes por Mensagem e Pacotes por Mensagem) + CPU
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    for i, proto in enumerate(protocolos):
        sub = df_agregado[df_agregado["protocolo"] == proto].sort_values("sensores")
        pos = x + (i - len(protocolos) / 2 + 0.5) * largura
        b1 = axes[0].bar(
            pos,
            sub["bytes_por_mensagem_mean"],
            largura,
            label=nomes_legenda.get(proto, proto),
            color=cores.get(proto)
        )
        axes[0].bar_label(b1, fmt="%.0f B", padding=3, fontsize=8)

        if "cpu_gerador_pct_mean" in sub.columns and sub["cpu_gerador_pct_mean"].notna().any():
            b2 = axes[1].bar(
                pos,
                sub["cpu_gerador_pct_mean"].fillna(0),
                largura,
                label=nomes_legenda.get(proto, proto),
                color=cores.get(proto)
            )
            axes[1].bar_label(b2, fmt="%.1f%%", padding=3, fontsize=8)

    axes[0].set_title("Tráfego de Rede (Bytes por Leitura)")
    axes[0].set_xticks(x)
    axes[0].set_xticklabels([f"{c} sensores" for c in cargas])
    axes[0].set_ylabel("Bytes / Mensagem")
    axes[0].legend()

    axes[1].set_title("Uso Médio de CPU do Gerador de Carga (%)")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels([f"{c} sensores" for c in cargas])
    axes[1].set_ylabel("Uso de CPU (%)")
    axes[1].legend()

    plt.tight_layout()
    caminho_g2 = os.path.join(diretorio_saida, "grafico_trafego_e_cpu.png")
    plt.savefig(caminho_g2, dpi=300)
    plt.close()


def avaliar_criterios_rn46_rn48(df_agregado: pd.DataFrame) -> dict:
    """Avalia RN46 (entrega >= 99% em 100s), RN47 (alerta p99 <= 5000 ms) e RN48 (desempate)."""
    # Filtra maior carga disponível (100 sensores, ou a máxima presente)
    carga_max = int(df_agregado["sensores"].max())
    sub = df_agregado[df_agregado["sensores"] == carga_max].copy()

    avaliacao = []
    for _, row in sub.iterrows():
        entrega_ok = bool(row["taxa_entrega_pct_mean"] >= 99.0)
        alerta_p99 = row.get("alerta_p99_ms_mean", np.nan)
        alerta_ok = bool(pd.isna(alerta_p99) or alerta_p99 <= 5000.0)
        qualificado = entrega_ok and alerta_ok

        avaliacao.append({
            "protocolo": row["protocolo"],
            "carga_avaliada_sensores": carga_max,
            "taxa_entrega_media_pct": round(float(row["taxa_entrega_pct_mean"]), 4),
            "rn46_entrega_maior_igual_99pct": entrega_ok,
            "alerta_p99_medio_ms": (
                round(float(alerta_p99), 3) if pd.notna(alerta_p99) else None
            ),
            "rn47_alerta_p99_menor_igual_5000ms": alerta_ok,
            "qualificado": qualificado,
            "bytes_por_mensagem_medio": round(float(row["bytes_por_mensagem_mean"]), 2),
            "latencia_p95_media_ms": round(float(row["latencia_p95_ms_mean"]), 3),
            "latencia_p99_media_ms": round(float(row["latencia_p99_ms_mean"]), 3)
        })

    qualificados = [a for a in avaliacao if a["qualificado"]]
    qualificados_ordenados = sorted(
        qualificados,
        key=lambda x: (x["bytes_por_mensagem_medio"], x["latencia_p95_media_ms"])
    )

    # Também destaca o melhor protocolo com entrega confirmada na camada de aplicação (QoS 1 vs HTTP)
    com_confirmacao = [
        a for a in qualificados if a["protocolo"] in ("mqtt_qos1", "http")
    ]
    com_confirmacao_ord = sorted(
        com_confirmacao,
        key=lambda x: (x["bytes_por_mensagem_medio"], x["latencia_p95_media_ms"])
    )

    return {
        "carga_referencia_sensores": carga_max,
        "vencedor_geral_rn48": (
            qualificados_ordenados[0]["protocolo"]
            if qualificados_ordenados
            else None
        ),
        "vencedor_com_confirmacao_entrega": (
            com_confirmacao_ord[0]["protocolo"] if com_confirmacao_ord else None
        ),
        "detalhes_por_protocolo": avaliacao
    }


def main():
    ap = argparse.ArgumentParser(
        description="Análise final consolidada das rodadas de coleta."
    )
    ap.add_argument(
        "--diretorio",
        default="dados/coleta_final",
        help="Pasta contendo os arquivos da coleta (padrão: dados/coleta_final)"
    )
    ap.add_argument(
        "--aquecimento",
        type=float,
        default=30.0,
        help="Segundos de aquecimento a descartar (padrão: 30)"
    )
    ap.add_argument(
        "--intervalo",
        type=float,
        default=5.0,
        help="Intervalo nominal entre leituras em segundos (padrão: 5)"
    )
    args = ap.parse_args()

    padrao_rec = os.path.join(args.diretorio, "recebidas_*.csv")
    arquivos_rec = sorted(glob.glob(padrao_rec))
    if not arquivos_rec:
        print(
            f"Nenhum arquivo 'recebidas_*.csv' encontrado em: {args.diretorio}",
            file=sys.stderr
        )
        sys.exit(1)

    caminho_progresso = os.path.join(args.diretorio, "coleta_progresso.csv")
    df_progresso = (
        pd.read_csv(caminho_progresso)
        if os.path.exists(caminho_progresso)
        else None
    )

    linhas = []
    for c in arquivos_rec:
        try:
            linhas.append(
                analisar_rodada(
                    args.diretorio,
                    c,
                    args.aquecimento,
                    args.intervalo,
                    df_progresso
                )
            )
        except Exception as e:  # noqa: BLE001
            print(f"Erro ao analisar {c}: {e}", file=sys.stderr)

    df_rodadas = pd.DataFrame(linhas).sort_values(
        ["sensores", "protocolo", "repeticao"]
    )

    # Agregação por Cenário (protocolo + sensores)
    metricas_num = [
        "taxa_entrega_pct",
        "perdas_exatas",
        "duplicatas",
        "latencia_media_ms",
        "latencia_mediana_ms",
        "latencia_p95_ms",
        "latencia_p99_ms",
        "latencia_max_ms",
        "alerta_medio_ms",
        "alerta_p95_ms",
        "alerta_p99_ms",
        "bytes_rede_total",
        "bytes_por_mensagem",
        "pacotes_por_mensagem",
        "cpu_gerador_pct",
        "agend_desvio_medio_ms"
    ]
    aggs = {m: ["mean", "std"] for m in metricas_num if m in df_rodadas.columns}
    df_agregado = df_rodadas.groupby(
        ["cenario_id", "protocolo", "sensores"], as_index=False
    ).agg(aggs)
    df_agregado.columns = [
        f"{col[0]}_{col[1]}" if col[1] else col[0]
        for col in df_agregado.columns
    ]

    # Salva CSVs e JSON de decisão
    arq_rodadas = os.path.join(args.diretorio, "resumo_final_por_rodada.csv")
    arq_agregado = os.path.join(args.diretorio, "resumo_final_agregado.csv")
    arq_decisao = os.path.join(args.diretorio, "decisao_final_rn46_rn48.json")

    df_rodadas.to_csv(arq_rodadas, index=False)
    df_agregado.to_csv(arq_agregado, index=False)

    decisao = avaliar_criterios_rn46_rn48(df_agregado)
    with open(arq_decisao, "w", encoding="utf-8") as f:
        json.dump(decisao, f, indent=2, ensure_ascii=False)

    gerar_graficos(df_agregado, args.diretorio)

    # Exibição formatada no terminal
    pd.set_option("display.width", 260)
    pd.set_option("display.max_columns", None)
    print("\n================ RESUMO POR RODADA ================")
    print(df_rodadas.T.to_string(header=False))

    print("\n================ DECISÃO (RN46–RN48) ================")
    print(json.dumps(decisao, indent=2, ensure_ascii=False))
    print(f"\nArquivos gerados com sucesso em '{args.diretorio}':")
    print(f"  - {os.path.basename(arq_rodadas)}")
    print(f"  - {os.path.basename(arq_agregado)}")
    print(f"  - {os.path.basename(arq_decisao)}")
    print("  - grafico_comparativo_latencias.png")
    print("  - grafico_trafego_e_cpu.png")


if __name__ == "__main__":
    main()