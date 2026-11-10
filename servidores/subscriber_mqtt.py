"""
Subscriber MQTT (paho-mqtt 2.x) que representa o lado servidor na arquitetura MQTT (RN20 a RN26).

Fluxo idêntico ao da API HTTP:
1. No callback on_message, marca imediatamente t_recebimento = time.perf_counter_ns() (RN21)
2. Chama processador.processar_payload(msg.payload, t_recebimento) do módulo comum (RN20)
3. Mensagens inválidas são descartadas e contadas à parte sem gerar leitura/alerta (RN23, RN24)
4. Mantém todos os registros em memória durante a rodada e grava em disco apenas no encerramento (RN26)
"""

import argparse
import json
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any
import paho.mqtt.client as mqtt

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from comum.modelo import TOPICO_MQTT_ASSINATURA
from comum.processamento import ProcessadorLeituras

TOPICO_CONTROLE_FINALIZAR = "lab/controle/finalizar"


class SubscriberMQTT:
    """Encapsula o cliente paho-mqtt 2.x do subscriber e o ProcessadorLeituras compartilhado."""

    def __init__(
        self,
        broker_host: str = "127.0.0.1",
        broker_port: int = 1883,
        qos: int = 0,
        cenario_id: str = "mqtt_qos0",
        repeticao: int = 1,
        diretorio_saida: str = "dados",
    ) -> None:
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.qos = qos
        self.cenario_id = cenario_id
        self.repeticao = repeticao
        self.diretorio_saida = diretorio_saida

        protocolo_label = f"MQTT_QOS{qos}"
        self.processador = ProcessadorLeituras(protocolo=protocolo_label)
        self._encerrado = threading.Event()
        self._gravado = False

        # paho-mqtt 2.x exige CallbackAPIVersion.VERSION2
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"subscriber_servidor_{protocolo_label.lower()}",
            clean_session=True,
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: Any = None,
    ) -> None:
        if reason_code == 0:
            # Assina o tópico dos sensores (lab/sala1/+/dados) com o QoS do cenário
            client.subscribe(TOPICO_MQTT_ASSINATURA, qos=self.qos)
            client.subscribe(TOPICO_CONTROLE_FINALIZAR, qos=1)

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        msg: mqtt.MQTTMessage,
    ) -> None:
        """
        Callback principal de recepção MQTT (Seção 1 e RN20-RN24):
        1. Marca t_recebimento com time.perf_counter_ns() como primeiro passo absoluto (RN21)
        2. Encaminha o payload para o mesmo processador usado pela API HTTP (RN20)
        """
        t_recebimento = time.time_ns()

        if msg.topic == TOPICO_CONTROLE_FINALIZAR:
            self._tratar_comando_finalizar(msg.payload)
            return

        # Caminho crítico idêntico ao da API HTTP: valida JSON, registra no buffer e avalia alerta
        # Se retornar False (RN23, RN24), o subscriber simplesmente descarta (já contabilizado em total_invalidas)
        self.processador.processar_payload(msg.payload, t_recebimento)

    def _tratar_comando_finalizar(self, payload: bytes) -> None:
        """Permite finalizar e gravar os dados via mensagem de controle ou encerramento do processo."""
        try:
            dados = json.loads(payload.decode("utf-8")) if payload else {}
        except Exception:
            dados = {}

        cenario = dados.get("cenario_id", self.cenario_id)
        rep = int(dados.get("repeticao", self.repeticao))
        pasta = dados.get("diretorio_saida", self.diretorio_saida)
        encerrar_processo = bool(dados.get("encerrar", False))

        self.processador.gravar_em_disco(
            cenario_id=cenario,
            repeticao=rep,
            diretorio_saida=pasta,
            limpar_memoria=True,
        )
        self._gravado = True
        if encerrar_processo:
            self._encerrado.set()

    def finalizar_e_gravar(self) -> dict[str, Any]:
        """Grava os buffers em disco no término da rodada caso ainda não tenham sido gravados."""
        if not self._gravado:
            self._gravado = True
            return self.processador.gravar_em_disco(
                cenario_id=self.cenario_id,
                repeticao=self.repeticao,
                diretorio_saida=self.diretorio_saida,
                limpar_memoria=True,
            )
        return self.processador.obter_estatisticas()

    def executar(self) -> None:
        """Conecta ao broker Mosquitto e mantém o loop de recepção até receber sinal de parada."""

        def _tratador_sinal(signum: int, frame: Any) -> None:
            self._encerrado.set()

        signal.signal(signal.SIGINT, _tratador_sinal)
        signal.signal(signal.SIGTERM, _tratador_sinal)

        self.client.connect(self.broker_host, self.broker_port, keepalive=60)
        self.client.loop_start()

        try:
            while not self._encerrado.is_set():
                self._encerrado.wait(timeout=0.5)
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            self.finalizar_e_gravar()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Subscriber MQTT para comparação com a API HTTP")
    parser.add_argument("--host", default="127.0.0.1", help="Endereço do broker Mosquitto")
    parser.add_argument("--port", type=int, default=1883, help="Porta do broker Mosquitto")
    parser.add_argument("--qos", type=int, choices=[0, 1], default=0, help="Nível de QoS (0 ou 1)")
    parser.add_argument("--cenario", default="mqtt_qos0", help="Identificador do cenário")
    parser.add_argument("--repeticao", type=int, default=1, help="Número da repetição")
    parser.add_argument("--saida", default="dados", help="Diretório de saída dos CSVs")
    args = parser.parse_args()

    sub = SubscriberMQTT(
        broker_host=args.host,
        broker_port=args.port,
        qos=args.qos,
        cenario_id=args.cenario,
        repeticao=args.repeticao,
        diretorio_saida=args.saida,
    )
    sub.executar()
    sys.exit(0)

