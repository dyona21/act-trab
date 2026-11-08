"""
Lógica compartilhada de validação, registro em memória e avaliação de alertas (RN09 a RN26).
Tanto a API HTTP (servidores/api_http.py) quanto o subscriber MQTT (servidores/subscriber_mqtt.py)
chamam exatamente as mesmas funções deste módulo.
"""

import csv
import json
import threading
import time
from pathlib import Path
from typing import Any
from pydantic import ValidationError

from comum.modelo import (
    TEMP_NORMAL_MAX,
    TEMP_NORMAL_MIN,
    UMID_NORMAL_MAX,
    UMID_NORMAL_MIN,
    Leitura,
)


class ProcessadorLeituras:
    """
    Gerencia o estado em memória de uma rodada no servidor (HTTP ou MQTT):
    - Valida o payload recebido (RN23, RN24)
    - Registra todas as recepções em buffer de memória sem escrita em disco no caminho crítico (RN22, RN26)
    - Mantém a máquina de estados de alertas por sensor (RN09, RN10, RN11, RN12, RN13, RN15)
    """

    def __init__(self, protocolo: str) -> None:
        self.protocolo = protocolo
        self._lock = threading.Lock()
        self.resetar()

    def resetar(self) -> None:
        """Zera todos os buffers e estados em memória para iniciar uma nova rodada (RN36)."""
        with self._lock:
            # Cada tupla: (protocolo, sensor_id, seq, t_envio, t_recebimento, disparou_alerta, duplicata, temperatura, umidade)
            self._buffer_leituras: list[tuple[Any, ...]] = []
            # Cada tupla: (protocolo, sensor_id, seq_abertura, tipo, t_envio, t_recebimento, t_alerta, tempo_alerta_ns)
            self._buffer_alertas: list[tuple[Any, ...]] = []
            # Controle de primeira recepção de cada par (sensor_id, seq) (RN22)
            self._pares_vistos: set[tuple[str, int]] = set()
            # Estado do alerta por sensor: True = alerta aberto, False = normal (RN10)
            self._alerta_aberto: dict[str, bool] = {}
            # Contadores de mensagens inválidas descartadas (RN23, RN24)
            self.total_invalidas: int = 0

    @staticmethod
    def avaliar_violacao(leitura: Leitura) -> tuple[bool, str]:
        """
        Verifica se a leitura viola os limiares normais (RN09, RN11).
        Faixa normal inclusiva: 2 °C <= temperatura <= 8 °C e 30% <= umidade <= 60%.
        Retorna (em_violacao, tipo_violacao).
        """
        violacao_temp = not (TEMP_NORMAL_MIN <= leitura.temperatura <= TEMP_NORMAL_MAX)
        violacao_umid = not (UMID_NORMAL_MIN <= leitura.umidade <= UMID_NORMAL_MAX)

        if violacao_temp and violacao_umid:
            return True, "ambos"
        if violacao_temp:
            return True, "temperatura"
        if violacao_umid:
            return True, "umidade"
        return False, "nenhum"

    def processar_payload(self, payload_bruto: bytes | str, t_recebimento: int) -> bool:
        """
        Sequência padronizada do caminho crítico (Seção 1 e RN20-RN26):
        1. t_recebimento já foi marcado na entrada do handler/callback com time.perf_counter_ns() (RN21)
        2. Validar JSON e faixa física do sensor (RN23, RN24)
        3. Registrar leitura no buffer em memória e avaliar alerta na 1ª recepção (RN22, RN10-RN15)

        Retorna True se a leitura é válida, ou False se for inválida (RN23/RN24).
        """
        try:
            leitura = Leitura.model_validate_json(payload_bruto)
        except (ValidationError, ValueError):
            with self._lock:
                self.total_invalidas += 1
            return False

        self.registrar_leitura_validada(leitura, t_recebimento)
        return True

    def registrar_leitura_validada(self, leitura: Leitura, t_recebimento: int) -> bool:
        """
        Registra uma instância já validada de Leitura e atualiza o estado de alerta.
        Retorna True se disparou um novo alerta, False caso contrário.
        """
        chave = (leitura.sensor_id, leitura.seq)
        disparou_alerta = False

        with self._lock:
            if chave in self._pares_vistos:
                # RN22: Toda recepção é registrada, inclusive duplicatas.
                # Para latência e alertas, vale apenas a primeira recepção.
                duplicata = True
            else:
                duplicata = False
                self._pares_vistos.add(chave)

                em_violacao, tipo_violacao = self.avaliar_violacao(leitura)
                alerta_atual = self._alerta_aberto.get(leitura.sensor_id, False)

                if em_violacao and not alerta_atual:
                    # RN10: Abre alerta na primeira leitura em violação
                    self._alerta_aberto[leitura.sensor_id] = True
                    disparou_alerta = True
                    # RN11, RN12, RN15: Marca instante do alerta com perf_counter_ns e registra em memória
                    t_alerta = time.perf_counter_ns()
                    tempo_alerta_ns = t_alerta - leitura.t_envio
                    self._buffer_alertas.append(
                        (
                            self.protocolo,
                            leitura.sensor_id,
                            leitura.seq,
                            tipo_violacao,
                            leitura.t_envio,
                            t_recebimento,
                            t_alerta,
                            tempo_alerta_ns,
                        )
                    )
                elif not em_violacao and alerta_atual:
                    # RN10: Fecha o alerta quando uma leitura volta à faixa normal
                    self._alerta_aberto[leitura.sensor_id] = False

            self._buffer_leituras.append(
                (
                    self.protocolo,
                    leitura.sensor_id,
                    leitura.seq,
                    leitura.t_envio,
                    t_recebimento,
                    disparou_alerta,
                    duplicata,
                    leitura.temperatura,
                    leitura.umidade,
                )
            )

        return disparou_alerta

    def obter_estatisticas(self) -> dict[str, Any]:
        """Retorna contadores atuais em memória (para diagnóstico fora do caminho crítico)."""
        with self._lock:
            total_recebidas = len(self._buffer_leituras)
            total_unicas = len(self._pares_vistos)
            return {
                "protocolo": self.protocolo,
                "total_recebidas": total_recebidas,
                "total_unicas": total_unicas,
                "total_duplicatas": total_recebidas - total_unicas,
                "total_invalidas": self.total_invalidas,
                "total_alertas": len(self._buffer_alertas),
            }

    def gravar_em_disco(
        self,
        cenario_id: str = "padrao",
        repeticao: int = 1,
        diretorio_saida: str | Path = "dados",
        limpar_memoria: bool = True,
    ) -> dict[str, Any]:
        """
        Descarrega os buffers de leituras e alertas em arquivos CSV no fim da rodada (RN25, RN26).
        """
        pasta = Path(diretorio_saida)
        pasta.mkdir(parents=True, exist_ok=True)

        prefixo = f"{cenario_id}_rep{repeticao}"
        arq_leituras = pasta / f"recebidas_{prefixo}.csv"
        arq_alertas = pasta / f"alertas_{prefixo}.csv"
        arq_resumo = pasta / f"resumo_servidor_{prefixo}.json"

        with self._lock:
            leituras_copia = list(self._buffer_leituras)
            alertas_copia = list(self._buffer_alertas)
            total_unicas = len(self._pares_vistos)
            total_invalidas = self.total_invalidas

        # Grava CSV de leituras recebidas
        with arq_leituras.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "protocolo",
                    "sensor_id",
                    "seq",
                    "t_envio",
                    "t_recebimento",
                    "disparou_alerta",
                    "duplicata",
                    "temperatura",
                    "umidade",
                ]
            )
            writer.writerows(leituras_copia)

        # Grava CSV de alertas gerados
        with arq_alertas.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "protocolo",
                    "sensor_id",
                    "seq_abertura",
                    "tipo",
                    "t_envio",
                    "t_recebimento",
                    "t_alerta",
                    "tempo_alerta_ns",
                ]
            )
            writer.writerows(alertas_copia)

        resumo = {
            "protocolo": self.protocolo,
            "cenario_id": cenario_id,
            "repeticao": repeticao,
            "total_recebidas": len(leituras_copia),
            "total_unicas": total_unicas,
            "total_duplicatas": len(leituras_copia) - total_unicas,
            "total_invalidas": total_invalidas,
            "total_alertas": len(alertas_copia),
            "arquivo_leituras": str(arq_leituras),
            "arquivo_alertas": str(arq_alertas),
        }

        with arq_resumo.open("w", encoding="utf-8") as f:
            json.dump(resumo, f, indent=2, ensure_ascii=False)

        if limpar_memoria:
            self.resetar()

        return resumo

