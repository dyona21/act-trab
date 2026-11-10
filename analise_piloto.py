"""Análise rápida de rodadas (pilotos da Fase 8).

Uso (na raiz do projeto, com o venv ativo):
    python analise_piloto.py dados\\fase8\\recebidas_http_10s_rep1.csv ^
                             dados\\fase8\\recebidas_mqtt_qos0_10s_rep1.csv ...
Opções:
    --aquecimento 10     segundos de aquecimento a descartar (padrão 30)
    --intervalo 5        intervalo entre leituras em segundos (padrão 5)
    --saida-csv X.csv    grava a tabela-resumo em CSV

Usa só o CSV de recebidas (e o de alertas, se existir ao lado).
- Perda: detectada por lacunas no número de sequência de cada sensor.
  Perdas nas últimas leituras de um sensor não são detectáveis só por aqui.
- O aquecimento é descartado por número de sequência (aprox.).
"""
import argparse
import glob
import math
import os
import sys

import numpy as np
import pandas as pd


def analisar(caminho, aquecimento, intervalo):
    df = pd.read_csv(caminho)
    protocolo = df["protocolo"].iloc[0]
    n_desc = math.ceil(aquecimento / intervalo)

    unicas = df[~df["duplicata"]].copy()
    unicas = unicas[unicas["seq"] >= n_desc]
    lat_ms = (unicas["t_recebimento"] - unicas["t_envio"]) / 1e6

    # Perdas por lacuna de seq
    esperadas = 0
    recebidas = 0
    for _, g in unicas.groupby("sensor_id"):
        seqs = set(g["seq"])
        faixa = range(n_desc, int(g["seq"].max()) + 1)
        esperadas += len(faixa)
        recebidas += len(seqs & set(faixa))
    perdas = esperadas - recebidas

    # Precisão do agendamento dos sensores (desvio do intervalo de 5 s)
    desvios = []
    for _, g in unicas.sort_values("seq").groupby("sensor_id"):
        t = g["t_envio"].to_numpy()
        consecutivas = np.diff(g["seq"].to_numpy()) == 1
        d = np.diff(t) / 1e6 - intervalo * 1000
        desvios.extend(np.abs(d[consecutivas]))
    desvios = np.array(desvios) if desvios else np.array([0.0])

    # Alertas (arquivo ao lado)
    alerta_ms = None
    caminho_alerta = caminho.replace("recebidas_", "alertas_")
    if os.path.exists(caminho_alerta):
        a = pd.read_csv(caminho_alerta)
        if len(a):
            alerta_ms = a["tempo_alerta_ns"] / 1e6

    res = {
        "arquivo": os.path.basename(caminho),
        "protocolo": protocolo,
        "sensores": unicas["sensor_id"].nunique(),
        "leituras_analisadas": len(unicas),
        "duplicatas": int(df["duplicata"].sum()),
        "perdas_por_lacuna": perdas,
        "latencia_media_ms": round(lat_ms.mean(), 3),
        "latencia_dp_ms": round(lat_ms.std(), 3),
        "latencia_mediana_ms": round(lat_ms.median(), 3),
        "latencia_p95_ms": round(np.percentile(lat_ms, 95), 3),
        "latencia_p99_ms": round(np.percentile(lat_ms, 99), 3),
        "latencia_max_ms": round(lat_ms.max(), 3),
        "latencias_negativas": int((lat_ms < 0).sum()),
        "agend_desvio_medio_ms": round(float(desvios.mean()), 2),
        "agend_desvio_max_ms": round(float(desvios.max()), 2),
        "agend_acima_50ms": int((desvios > 50).sum()),
        "alerta_medio_ms": round(float(alerta_ms.mean()), 3) if alerta_ms is not None else None,
        "alerta_max_ms": round(float(alerta_ms.max()), 3) if alerta_ms is not None else None,
    }
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("arquivos", nargs="+")
    ap.add_argument("--aquecimento", type=float, default=30)
    ap.add_argument("--intervalo", type=float, default=5)
    ap.add_argument("--saida-csv")
    args = ap.parse_args()

    arquivos = []
    for padrao in args.arquivos:  # o cmd do Windows não expande curingas
        arquivos.extend(sorted(glob.glob(padrao)) or [padrao])

    linhas = []
    for c in arquivos:
        try:
            linhas.append(analisar(c, args.aquecimento, args.intervalo))
        except Exception as e:  # noqa: BLE001
            print(f"Erro em {c}: {e}", file=sys.stderr)

    if not linhas:
        sys.exit(1)

    tabela = pd.DataFrame(linhas)
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", None)
    print(tabela.T.to_string(header=False))
    if args.saida_csv:
        tabela.to_csv(args.saida_csv, index=False)
        print("\nResumo gravado em", args.saida_csv)


if __name__ == "__main__":
    main()