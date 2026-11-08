"""
Análise de métricas, agregação de cenários, verificação dos critérios de decisão e gráficos (RN12-RN14, RN31, RN34, RN40-RN49).
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from comum.modelo import (
    CRITERIO_TAXA_ENTREGA_MIN_PCT,
    CRITERIO_TEMPO_ALERTA_P99_MAX_MS,
)

# RN40: Método explícito de cálculo de percentil (interpolação linear padrão numpy/pandas)
METODO_PERCENTIL = "linear"


def analisar_rodada(
    cenario_id: str,
    repeticao: int,
    diretorio_dados: str | Path = "dados",
    bytes_totais_captura: int | None = None,
) -> dict[str, Any]:
    """
    Calcula todas as métricas de uma única rodada (RN40 a RN44), aplicando:
    - Exclusão dos 30 s iniciais de aquecimento em todas as métricas (RN34)
    - Descarte de mensagens recebidas após 10 s do fim da rodada (RN31)
    - Uso apenas da 1ª recepção para latência e entrega única (RN22, RN40, RN41)
    - Contabilização de duplicatas além da 1ª recepção (RN42)
    - Contabilização de excursões sem nenhuma leitura recebida como falha de detecção (RN14)
    """
    pasta = Path(diretorio_dados)
    prefixo = f"{cenario_id}_rep{repeticao}"

    arq_meta = pasta / f"metadados_{prefixo}.json"
    arq_env = pasta / f"enviadas_{prefixo}.csv"
    arq_rec = pasta / f"recebidas_{prefixo}.csv"
    arq_ale = pasta / f"alertas_{prefixo}.csv"
    arq_exc = pasta / f"excursoes_{prefixo}.csv"

    with arq_meta.open("r", encoding="utf-8") as f:
        meta = json.load(f)

    df_env = pd.read_csv(arq_env)
    df_rec = pd.read_csv(arq_rec)
    df_ale = pd.read_csv(arq_ale) if arq_ale.exists() else pd.DataFrame()
    df_exc = pd.read_csv(arq_exc) if arq_exc.exists() else pd.DataFrame()

    t_fim_aquecimento_ns = int(meta["t_fim_aquecimento_ns"])
    t_limite_recepcao_ns = int(meta["t_limite_recepcao_ns"])

    # RN34: Exclui leituras com t_envio dentro dos 30 s iniciais de aquecimento
    df_env_val = df_env[df_env["t_envio"] >= t_fim_aquecimento_ns].copy()
    total_enviadas = len(df_env_val)
    bytes_payload_total = int(df_env_val["bytes_payload"].sum()) if total_enviadas > 0 else 0

    # Filtra recepções válidas: pertencentes às leituras pós-aquecimento (RN34) e dentro do limite de 10 s (RN31)
    df_rec_val = df_rec[
        (df_rec["t_envio"] >= t_fim_aquecimento_ns)
        & (df_rec["t_recebimento"] <= t_limite_recepcao_ns)
    ].copy()

    # RN22: Primeira recepção vs duplicatas entre as recepções válidas
    df_rec_val = df_rec_val.sort_values("t_recebimento", kind="mergesort")
    mask_duplicata = df_rec_val.duplicated(subset=["sensor_id", "seq"], keep="first")
    df_unicas = df_rec_val[~mask_duplicata].copy()
    qtd_unicas = len(df_unicas)
    qtd_duplicatas = int(mask_duplicata.sum())

    # RN40: Latência = t_rx - t_envio em ms, usando apenas a primeira recepção
    if qtd_unicas > 0:
        latencias_ms = (df_unicas["t_recebimento"] - df_unicas["t_envio"]) / 1_000_000.0
        lat_media = float(latencias_ms.mean())
        lat_dp = float(latencias_ms.std(ddof=1)) if qtd_unicas > 1 else 0.0
        lat_mediana = float(np.percentile(latencias_ms, 50, method=METODO_PERCENTIL))
        lat_p95 = float(np.percentile(latencias_ms, 95, method=METODO_PERCENTIL))
        lat_p99 = float(np.percentile(latencias_ms, 99, method=METODO_PERCENTIL))
    else:
        lat_media = lat_dp = lat_mediana = lat_p95 = lat_p99 = float("nan")

    # RN41: Taxa de entrega e perda
    taxa_entrega_pct = (qtd_unicas / total_enviadas * 100.0) if total_enviadas > 0 else 0.0
    taxa_perda_pct = 100.0 - taxa_entrega_pct

    # RN42: Duplicatas divididas pelo total de leituras enviadas
    razao_duplicatas = (qtd_duplicatas / total_enviadas) if total_enviadas > 0 else 0.0
    taxa_duplicatas_pct = razao_duplicatas * 100.0

    # RN12, RN13, RN14: Tempo de alerta e falhas de detecção de excursões
    if not df_ale.empty:
        df_ale_val = df_ale[
            (df_ale["t_envio"] >= t_fim_aquecimento_ns)
            & (df_ale["t_recebimento"] <= t_limite_recepcao_ns)
        ].copy()
    else:
        df_ale_val = pd.DataFrame()

    if not df_ale_val.empty:
        tempos_alerta_ms = df_ale_val["tempo_alerta_ns"] / 1_000_000.0
        alerta_media_ms = float(tempos_alerta_ms.mean())
        alerta_p95_ms = float(np.percentile(tempos_alerta_ms, 95, method=METODO_PERCENTIL))
        alerta_p99_ms = float(np.percentile(tempos_alerta_ms, 99, method=METODO_PERCENTIL))
    else:
        alerta_media_ms = alerta_p95_ms = alerta_p99_ms = 0.0

    # RN14: Verifica excursões programadas das quais nenhuma leitura chegou ao servidor
    total_excursoes = len(df_exc) if not df_exc.empty else 0
    excursoes_nao_detectadas = 0
    if total_excursoes > 0 and not df_env_val.empty:
        pares_recebidos = set(zip(df_unicas["sensor_id"], df_unicas["seq"]))
        for _, exc_row in df_exc.iterrows():
            exc_id = exc_row["excursao_id"]
            leituras_exc = df_env_val[df_env_val["excursao_id"] == exc_id]
            chegou_alguma = any(
                (r["sensor_id"], r["seq"]) in pares_recebidos for _, r in leituras_exc.iterrows()
            )
            if not chegou_alguma:
                excursoes_nao_detectadas += 1

    # RN43, RN44: Tráfego, bytes por mensagem e overhead
    bytes_captura = bytes_totais_captura or meta.get("bytes_totais_captura")
    if bytes_captura and bytes_captura > 0 and total_enviadas > 0:
        bytes_por_msg = float(bytes_captura) / total_enviadas
        overhead_pct = ((bytes_captura - bytes_payload_total) / float(bytes_captura)) * 100.0
    else:
        bytes_por_msg = float("nan")
        overhead_pct = float("nan")

    return {
        "cenario_id": cenario_id,
        "protocolo": meta["protocolo"],
        "num_sensores": meta["num_sensores"],
        "repeticao": repeticao,
        "rodada_valida": meta.get("rodada_valida", True),
        "total_enviadas": total_enviadas,
        "leituras_unicas_recebidas": qtd_unicas,
        "duplicatas_recebidas": qtd_duplicatas,
        "taxa_entrega_pct": round(taxa_entrega_pct, 4),
        "taxa_perda_pct": round(taxa_perda_pct, 4),
        "razao_duplicatas": round(razao_duplicatas, 6),
        "taxa_duplicatas_pct": round(taxa_duplicatas_pct, 4),
        "latencia_media_ms": round(lat_media, 4),
        "latencia_dp_ms": round(lat_dp, 4),
        "latencia_mediana_ms": round(lat_mediana, 4),
        "latencia_p95_ms": round(lat_p95, 4),
        "latencia_p99_ms": round(lat_p99, 4),
        "tempo_alerta_medio_ms": round(alerta_media_ms, 4),
        "tempo_alerta_p95_ms": round(alerta_p95_ms, 4),
        "tempo_alerta_p99_ms": round(alerta_p99_ms, 4),
        "total_excursoes_programadas": total_excursoes,
        "alertas_nao_gerados_rn14": excursoes_nao_detectadas,
        "bytes_payload_total": bytes_payload_total,
        "bytes_totais_captura": bytes_captura,
        "bytes_por_mensagem": round(bytes_por_msg, 2) if not np.isnan(bytes_por_msg) else None,
        "overhead_pct": round(overhead_pct, 2) if not np.isnan(overhead_pct) else None,
    }


def agregar_cenarios(resultados_rodadas: list[dict[str, Any]]) -> pd.DataFrame:
    """
    RN45: Cada rodada gera um valor por métrica. O resultado de um cenário é a média
    e o desvio-padrão entre as 5 repetições.
    """
    df = pd.DataFrame(resultados_rodadas)
    metricas = [
        "taxa_entrega_pct",
        "taxa_perda_pct",
        "taxa_duplicatas_pct",
        "latencia_media_ms",
        "latencia_mediana_ms",
        "latencia_p95_ms",
        "latencia_p99_ms",
        "tempo_alerta_p99_ms",
        "bytes_por_mensagem",
        "overhead_pct",
    ]
    metricas_presentes = [m for m in metricas if m in df.columns]

    agrupado = df.groupby(["cenario_id", "protocolo", "num_sensores"], as_index=False).agg(
        {m: ["mean", "std"] for m in metricas_presentes}
    )
    agrupado.columns = [
        f"{col[0]}_{col[1]}" if col[1] else col[0] for col in agrupado.columns.to_flat_index()
    ]
    return agrupado


def avaliar_criterios_decisao(df_cenarios: pd.DataFrame) -> dict[str, Any]:
    """
    Avalia os critérios de decisão (RN46, RN47, RN48) nos cenários de 100 sensores:
    - RN46: Taxa de entrega mínima de 99% com 100 sensores
    - RN47: Tempo de alerta p99 <= 5000 ms (5 s)
    - RN48: Entre os qualificados, vence o de menor tráfego; desempate pela menor latência p95
    """
    df_100 = df_cenarios[df_cenarios["num_sensores"] == 100].copy()
    if df_100.empty:
        df_100 = df_cenarios.copy()

    qualificados = []
    desqualificados = []

    for _, row in df_100.iterrows():
        proto = row["protocolo"]
        entrega = row.get("taxa_entrega_pct_mean", 0.0)
        alerta_p99 = row.get("tempo_alerta_p99_ms_mean", 0.0)
        motivos = []

        if entrega < CRITERIO_TAXA_ENTREGA_MIN_PCT:
            motivos.append(
                f"RN46 violada: entrega {entrega:.2f}% < {CRITERIO_TAXA_ENTREGA_MIN_PCT}%"
            )
        if alerta_p99 > CRITERIO_TEMPO_ALERTA_P99_MAX_MS:
            motivos.append(
                f"RN47 violada: tempo de alerta p99 {alerta_p99:.2f} ms > {CRITERIO_TEMPO_ALERTA_P99_MAX_MS} ms"
            )

        item = {
            "protocolo": proto,
            "cenario_id": row["cenario_id"],
            "taxa_entrega_media_pct": round(float(entrega), 4),
            "tempo_alerta_p99_medio_ms": round(float(alerta_p99), 4),
            "bytes_por_mensagem_medio": (
                None
                if pd.isna(row.get("bytes_por_mensagem_mean"))
                else round(float(row["bytes_por_mensagem_mean"]), 2)
            ),
            "latencia_p95_media_ms": round(float(row.get("latencia_p95_ms_mean", 0.0)), 4),
            "motivos_desqualificacao": motivos,
        }
        if motivos:
            desqualificados.append(item)
        else:
            qualificados.append(item)

    # RN48: Ordena por menor tráfego (se disponível) e desempata por menor latência p95
    qualificados.sort(
        key=lambda x: (
            x["bytes_por_mensagem_medio"]
            if x["bytes_por_mensagem_medio"] is not None
            else float("inf"),
            x["latencia_p95_media_ms"],
        )
    )

    vencedor = qualificados[0] if qualificados else None
    return {
        "vencedor_rn48": vencedor,
        "qualificados": qualificados,
        "desqualificados": desqualificados,
    }


def gerar_graficos(df_rodadas: pd.DataFrame, pasta_saida: str | Path = "dados") -> list[str]:
    """Gera gráficos comparativos de latência p95/p99 e taxa de entrega por número de sensores."""
    pasta = Path(pasta_saida)
    pasta.mkdir(parents=True, exist_ok=True)
    arquivos_gerados: list[str] = []

    if df_rodadas.empty:
        return arquivos_gerados

    # Gráfico 1: Latência p95 por quantidade de sensores e protocolo
    fig, ax = plt.subplots(figsize=(8, 5))
    for proto, grp in df_rodadas.groupby("protocolo"):
        resumo = grp.groupby("num_sensores")["latencia_p95_ms"].agg(["mean", "std"]).reset_index()
        ax.errorbar(
            resumo["num_sensores"],
            resumo["mean"],
            yerr=resumo["std"].fillna(0.0),
            marker="o",
            capsize=4,
            label=proto.upper(),
        )
    ax.set_title("Latência p95 (ms) por Número de Sensores e Protocolo (RN40)")
    ax.set_xlabel("Número de Sensores")
    ax.set_ylabel("Latência p95 (ms)")
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend()
    arq_lat = pasta / "grafico_latencia_p95.png"
    fig.tight_layout()
    fig.savefig(arq_lat, dpi=150)
    plt.close(fig)
    arquivos_gerados.append(str(arq_lat))

    # Gráfico 2: Taxa de Entrega (%)
    fig, ax = plt.subplots(figsize=(8, 5))
    for proto, grp in df_rodadas.groupby("protocolo"):
        resumo = grp.groupby("num_sensores")["taxa_entrega_pct"].agg(["mean", "std"]).reset_index()
        ax.errorbar(
            resumo["num_sensores"],
            resumo["mean"],
            yerr=resumo["std"].fillna(0.0),
            marker="s",
            capsize=4,
            label=proto.upper(),
        )
    ax.axhline(
        CRITERIO_TAXA_ENTREGA_MIN_PCT,
        color="red",
        linestyle="--",
        label=f"Mínimo RN46 ({CRITERIO_TAXA_ENTREGA_MIN_PCT}%)",
    )
    ax.set_title("Taxa de Entrega (%) por Número de Sensores e Protocolo (RN41, RN46)")
    ax.set_xlabel("Número de Sensores")
    ax.set_ylabel("Taxa de Entrega (%)")
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend()
    arq_ent = pasta / "grafico_taxa_entrega.png"
    fig.tight_layout()
    fig.savefig(arq_ent, dpi=150)
    plt.close(fig)
    arquivos_gerados.append(str(arq_ent))

    return arquivos_gerados


def processar_diretorio_dados(diretorio_dados: str = "dados") -> dict[str, Any]:
    """Varre todos os arquivos metadados_*.json no diretório, analisa as rodadas e consolida."""
    pasta = Path(diretorio_dados)
    arquivos_meta = sorted(pasta.glob("metadados_*_rep*.json"))
    resultados_rodadas: list[dict[str, Any]] = []

    for arq in arquivos_meta:
        with arq.open("r", encoding="utf-8") as f:
            meta = json.load(f)
        cenario_id = meta["cenario_id"]
        repeticao = int(meta["repeticao"])
        arq_rec = pasta / f"recebidas_{cenario_id}_rep{repeticao}.csv"
        if arq_rec.exists():
            res = analisar_rodada(cenario_id, repeticao, diretorio_dados=pasta)
            resultados_rodadas.append(res)

    if not resultados_rodadas:
        return {"status": "sem_dados", "total_rodadas_analisadas": 0}

    df_rodadas = pd.DataFrame(resultados_rodadas)
    arq_metricas_rodadas = pasta / "metricas_por_rodada.csv"
    df_rodadas.to_csv(arq_metricas_rodadas, index=False)

    df_cenarios = agregar_cenarios(resultados_rodadas)
    arq_metricas_cenarios = pasta / "metricas_agregadas_cenarios.csv"
    df_cenarios.to_csv(arq_metricas_cenarios, index=False)

    decisao = avaliar_criterios_decisao(df_cenarios)
    arq_decisao = pasta / "decisao_rn46_rn48.json"
    with arq_decisao.open("w", encoding="utf-8") as f:
        json.dump(decisao, f, indent=2, ensure_ascii=False)

    graficos = gerar_graficos(df_rodadas, pasta_saida=pasta)

    return {
        "status": "ok",
        "metodo_percentil_rn40": METODO_PERCENTIL,
        "total_rodadas_analisadas": len(resultados_rodadas),
        "arquivo_rodadas": str(arq_metricas_rodadas),
        "arquivo_cenarios": str(arq_metricas_cenarios),
        "arquivo_decisao": str(arq_decisao),
        "graficos": graficos,
        "decisao": decisao,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analisador de métricas e gráficos (RN40 a RN49)")
    parser.add_argument("--dados", default="dados", help="Diretório com os CSVs e metadados das rodadas")
    args = parser.parse_args()

    relatorio = processar_diretorio_dados(args.dados)
    print(json.dumps(relatorio, indent=2, ensure_ascii=False))

