"""
Simulador de sensores DHT22 e gerador determinístico de cronograma de excursões (RN01 a RN19).
Garante que a mesma semente produza exatamente as mesmas defasagens iniciais e excursões
nos três protocolos (MQTT QoS 0, MQTT QoS 1 e HTTP/REST).
"""

import csv
import json
import math
import random
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from comum.modelo import (
    DURACAO_EXCURSAO_S,
    DURACAO_TOTAL_RODADA_S,
    FRACAO_SENSORES_EXCURSAO,
    INTERVALO_ENVIO_S,
    INTERVALO_EXCURSAO_S,
    TEMP_DESVIO_PADRAO,
    TEMP_EXCURSAO_MAX,
    TEMP_EXCURSAO_MIN,
    TEMP_FISICA_MAX,
    TEMP_FISICA_MIN,
    TEMP_NOMINAL,
    TEMP_NORMAL_MAX,
    TEMP_NORMAL_MIN,
    TEMPO_AQUECIMENTO_S,
    TOLERANCIA_FIM_S,
    UMID_DESVIO_PADRAO,
    UMID_EXCURSAO_MAX,
    UMID_EXCURSAO_MIN,
    UMID_FISICA_MAX,
    UMID_FISICA_MIN,
    UMID_NOMINAL,
    UMID_NORMAL_MAX,
    UMID_NORMAL_MIN,
)


@dataclass(frozen=True)
class ExcursaoProgramada:
    """Representa uma excursão injetada em um sensor (RN16 a RN19)."""

    excursao_id: int
    sensor_id: str
    inicio_relativo_s: float
    fim_relativo_s: float
    duracao_s: float
    tipo: str  # "temperatura", "umidade" ou "ambos"


def formatar_sensor_id(indice_1_based: int) -> str:
    """Formata o identificador do sensor conforme RN01: sensor01 a sensor100."""
    if not (1 <= indice_1_based <= 100):
        raise ValueError("Índice do sensor deve estar entre 1 e 100 (RN01).")
    return f"sensor{indice_1_based:02d}"


def gerar_defasagens_e_excursoes(
    num_sensores: int,
    semente: int,
    duracao_total_s: float = DURACAO_TOTAL_RODADA_S,
    aquecimento_s: float = TEMPO_AQUECIMENTO_S,
) -> tuple[dict[str, float], list[ExcursaoProgramada]]:
    """
    Gera de forma determinística (a partir de `semente`):
    1. A defasagem inicial de cada sensor entre 0 e 5 s (RN03)
    2. O cronograma de excursões programadas (RN16 a RN19):
       - 10% dos sensores (arredondado para cima)
       - Uma excursão a cada 120 s com duração de 30 s
       - Proibidas nos 30 s de aquecimento inicial e nos últimos 10 s da rodada (RN19)
    """
    rng = random.Random(semente)
    sensores = [formatar_sensor_id(i) for i in range(1, num_sensores + 1)]

    # RN03: Defasagem inicial entre 0 e 5 s com a mesma semente
    defasagens = {s: round(rng.uniform(0.0, INTERVALO_ENVIO_S), 6) for s in sensores}

    # RN16: 10% dos sensores (arredondando para cima)
    qtd_excursao = max(1, math.ceil(num_sensores * FRACAO_SENSORES_EXCURSAO))
    sensores_escolhidos = sorted(rng.sample(sensores, qtd_excursao))

    excursoes: list[ExcursaoProgramada] = []
    tipos_possiveis = ["temperatura", "umidade", "ambos"]

    # RN19: Excursões só podem ocorrer após `aquecimento_s` e terminar até `duracao_total_s - TOLERANCIA_FIM_S`
    limite_fim_s = duracao_total_s - TOLERANCIA_FIM_S
    excursao_id = 1

    for idx_s, sensor_id in enumerate(sensores_escolhidos):
        # Escalonamento determinístico dentro de cada janela de 120 s após o aquecimento
        t_janela = aquecimento_s + 10.0
        ciclo = 0
        while t_janela + DURACAO_EXCURSAO_S <= limite_fim_s:
            inicio_s = round(t_janela, 3)
            fim_s = round(inicio_s + DURACAO_EXCURSAO_S, 3)
            tipo = tipos_possiveis[(idx_s + ciclo) % len(tipos_possiveis)]
            excursoes.append(
                ExcursaoProgramada(
                    excursao_id=excursao_id,
                    sensor_id=sensor_id,
                    inicio_relativo_s=inicio_s,
                    fim_relativo_s=fim_s,
                    duracao_s=DURACAO_EXCURSAO_S,
                    tipo=tipo,
                )
            )
            excursao_id += 1
            ciclo += 1
            t_janela += INTERVALO_EXCURSAO_S

    return defasagens, excursoes


def salvar_cronograma_excursoes(
    excursoes: list[ExcursaoProgramada],
    caminho_csv: str | Path,
) -> None:
    """Grava o cronograma de excursões em arquivo CSV (RN18)."""
    caminho = Path(caminho_csv)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "excursao_id",
                "sensor_id",
                "inicio_relativo_s",
                "fim_relativo_s",
                "duracao_s",
                "tipo",
            ]
        )
        for exc in excursoes:
            writer.writerow(
                [
                    exc.excursao_id,
                    exc.sensor_id,
                    exc.inicio_relativo_s,
                    exc.fim_relativo_s,
                    exc.duracao_s,
                    exc.tipo,
                ]
            )


class SimuladorSensorDHT22:
    """
    Gera leituras simuladas de um único sensor DHT22 (RN01 a RN08, RN16, RN17).
    Possui seu próprio gerador pseudoaleatório determinístico por (semente, sensor_id).
    """

    def __init__(
        self,
        sensor_id: str,
        semente_base: int,
        defasagem_inicial_s: float,
        excursoes_do_sensor: list[ExcursaoProgramada],
    ) -> None:
        self.sensor_id = sensor_id
        self.defasagem_inicial_s = defasagem_inicial_s
        self.excursoes = excursoes_do_sensor
        self.seq = 0
        # RNG dedicado ao sensor para que cada leitura (sensor_id, seq) tenha os mesmos valores nos 3 protocolos
        semente_sensor = f"{semente_base}_{sensor_id}"
        self._rng = random.Random(semente_sensor)

    def reiniciar_sequencia(self) -> None:
        """Reinicia o contador seq em 0 para uma nova rodada (RN05)."""
        self.seq = 0

    def _excursao_ativa(self, tempo_relativo_s: float) -> ExcursaoProgramada | None:
        for exc in self.excursoes:
            if exc.inicio_relativo_s <= tempo_relativo_s < exc.fim_relativo_s:
                return exc
        return None

    def gerar_leitura(self, tempo_relativo_s: float) -> tuple[dict[str, Any], bytes, int | None]:
        """
        Gera o dicionário e o payload JSON codificado para o envio atual:
        - Marca t_envio imediatamente antes do envio com time.perf_counter_ns() (RN06)
        - Aplica ruído gaussiano e resolução de 0,1 (RN07, RN08)
        - Aplica valores de excursão quando dentro de uma janela programada (RN17)
        Retorna (dicionario_leitura, payload_json_bytes, excursao_id_ativa).
        """
        exc = self._excursao_ativa(tempo_relativo_s)

        if exc is not None and exc.tipo in ("temperatura", "ambos"):
            # RN17: Durante a excursão de temperatura, fica entre 9 e 12 °C
            temp = self._rng.uniform(TEMP_EXCURSAO_MIN, TEMP_EXCURSAO_MAX)
        else:
            # RN08: Condição normal em torno de 5 °C, mantida estritamente dentro da faixa normal [2.0, 8.0]
            temp = self._rng.gauss(TEMP_NOMINAL, TEMP_DESVIO_PADRAO)
            temp = max(TEMP_NORMAL_MIN + 0.1, min(TEMP_NORMAL_MAX - 0.1, temp))

        if exc is not None and exc.tipo in ("umidade", "ambos"):
            # RN17: Durante a excursão de umidade, fica entre 65 e 75%
            umid = self._rng.uniform(UMID_EXCURSAO_MIN, UMID_EXCURSAO_MAX)
        else:
            # RN08: Condição normal em torno de 45%, mantida estritamente dentro da faixa normal [30.0, 60.0]
            umid = self._rng.gauss(UMID_NOMINAL, UMID_DESVIO_PADRAO)
            umid = max(UMID_NORMAL_MIN + 0.5, min(UMID_NORMAL_MAX - 0.5, umid))

        # RN07: Faixa física do DHT22 e resolução de 0,1
        temperatura = round(max(TEMP_FISICA_MIN, min(TEMP_FISICA_MAX, temp)), 1)
        umidade = round(max(UMID_FISICA_MIN, min(UMID_FISICA_MAX, umid)), 1)

        seq_atual = self.seq
        self.seq += 1

        # RN06: t_envio marcado imediatamente antes do envio em nanossegundos (perf_counter_ns)
        t_envio = time.perf_counter_ns()

        dados = {
            "sensor_id": self.sensor_id,
            "seq": seq_atual,
            "t_envio": t_envio,
            "temperatura": temperatura,
            "umidade": umidade,
        }
        # JSON enxuto sem espaços desnecessários para não inflar o payload nos dois lados
        payload_bytes = json.dumps(dados, separators=(",", ":")).encode("utf-8")
        excursao_id = exc.excursao_id if exc is not None else None
        return dados, payload_bytes, excursao_id

