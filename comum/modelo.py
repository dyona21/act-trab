"""
Modelos de dados e constantes das Regras de Negócio (RN01 a RN49).
Compartilhado integralmente entre a API HTTP e o subscriber MQTT (RN20).
"""

import re
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt, field_validator

# ============================================================================
# Constantes das Regras de Negócio
# ============================================================================

# RN01: Sala fixa e formato de identificador
SALA_FIXA = "sala1"
PADRAO_SENSOR_ID = re.compile(r"^sensor(?:0[1-9]|[1-9][0-9]|100)$")

# RN02: Intervalo de envio sem deriva (5 segundos)
INTERVALO_ENVIO_S = 5.0
INTERVALO_ENVIO_NS = int(INTERVALO_ENVIO_S * 1_000_000_000)

# RN07: Limites físicos do sensor DHT22
TEMP_FISICA_MIN = -40.0
TEMP_FISICA_MAX = 80.0
UMID_FISICA_MIN = 0.0
UMID_FISICA_MAX = 100.0
RESOLUCAO_DHT22 = 0.1

# RN08: Condição nominal de operação
TEMP_NOMINAL = 5.0
TEMP_DESVIO_PADRAO = 0.4
UMID_NOMINAL = 45.0
UMID_DESVIO_PADRAO = 2.0

# RN09: Faixa normal (limites inclusivos)
TEMP_NORMAL_MIN = 2.0
TEMP_NORMAL_MAX = 8.0
UMID_NORMAL_MIN = 30.0
UMID_NORMAL_MAX = 60.0

# RN16, RN17, RN19: Excursões programadas
FRACAO_SENSORES_EXCURSAO = 0.10
INTERVALO_EXCURSAO_S = 120.0
DURACAO_EXCURSAO_S = 30.0
TEMP_EXCURSAO_MIN = 9.0
TEMP_EXCURSAO_MAX = 12.0
UMID_EXCURSAO_MIN = 65.0
UMID_EXCURSAO_MAX = 75.0

# RN19, RN31, RN33, RN34: Tempos de rodada
TEMPO_AQUECIMENTO_S = 30.0
DURACAO_MEDICAO_S = 600.0  # 10 minutos
DURACAO_TOTAL_RODADA_S = TEMPO_AQUECIMENTO_S + DURACAO_MEDICAO_S  # 630 s
TOLERANCIA_FIM_S = 10.0  # RN19 e RN31

# RN28: Timeout HTTP
TIMEOUT_HTTP_S = 5.0

# RN37, RN39: Limites de aceitação da rodada
LIMITE_CPU_GERADOR_PCT = 80.0
ATRASO_MAX_AGENDAMENTO_MS = 50.0

# RN46, RN47: Critérios de decisão
CRITERIO_TAXA_ENTREGA_MIN_PCT = 99.0
CRITERIO_TEMPO_ALERTA_P99_MAX_MS = 5000.0


def topico_mqtt_sensor(sensor_id: str) -> str:
    """Retorna o tópico MQTT conforme RN01: lab/sala1/<sensor_id>/dados."""
    return f"lab/{SALA_FIXA}/{sensor_id}/dados"


TOPICO_MQTT_ASSINATURA = f"lab/{SALA_FIXA}/+/dados"


class Leitura(BaseModel):
    """
    Modelo de validação de leitura do sensor DHT22 (RN01, RN04, RN05, RN06, RN07, RN23, RN24).
    Proíbe campos extras e conversão implícita de strings/booleanos para números.
    """

    model_config = ConfigDict(extra="forbid")

    sensor_id: str
    seq: StrictInt = Field(ge=0)
    t_envio: StrictInt = Field(gt=0)
    temperatura: StrictFloat | StrictInt = Field(ge=TEMP_FISICA_MIN, le=TEMP_FISICA_MAX)
    umidade: StrictFloat | StrictInt = Field(ge=UMID_FISICA_MIN, le=UMID_FISICA_MAX)

    @field_validator("sensor_id")
    @classmethod
    def validar_sensor_id(cls, v: str) -> str:
        if not PADRAO_SENSOR_ID.match(v):
            raise ValueError(
                f"sensor_id inválido '{v}'. Deve seguir o formato sensor01 a sensor100 (RN01)."
            )
        return v

    @field_validator("temperatura", "umidade")
    @classmethod
    def converter_para_float(cls, v: float | int) -> float:
        return float(v)


class FinalizarRequisicao(BaseModel):
    """Parâmetros opcionais para o endpoint POST /finalizar (RN25, RN26)."""

    cenario_id: Optional[str] = "padrao"
    repeticao: Optional[int] = 1
    diretorio_saida: Optional[str] = "dados"
    limpar_memoria: bool = True

