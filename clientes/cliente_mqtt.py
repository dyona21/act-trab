"""
Cliente MQTT por sensor (paho-mqtt 2.x) (RN01, RN27, RN29, RN30).
- Cada sensor simulado possui sua própria instância mqtt.Client e conexão TCP (RN29)
- Publica no tópico lab/sala1/<sensor_id>/dados com QoS 0 ou QoS 1 (RN01, RN32)
- Sem fila offline na aplicação: se estiver desconectado no instante do envio, a leitura é
  contada como enviada e perdida enquanto o cliente tenta reconectar (RN27, RN30)
"""

import threading
from typing import Any
import paho.mqtt.client as mqtt

from comum.modelo import topico_mqtt_sensor


class ClienteMQTTSensor:
    """Mantém uma conexão MQTT persistente exclusiva para um único sensor (RN29)."""

    def __init__(
        self,
        sensor_id: str,
        broker_host: str = "127.0.0.1",
        broker_port: int = 1883,
        qos: int = 0,
    ) -> None:
        self.sensor_id = sensor_id
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.qos = qos
        self.topico = topico_mqtt_sensor(sensor_id)
        self._conectado = threading.Event()

        # paho-mqtt 2.x com CallbackAPIVersion.VERSION2
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"pub_{sensor_id}_qos{qos}",
            clean_session=True,
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.reconnect_delay_set(min_delay=1, max_delay=5)

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: Any = None,
    ) -> None:
        if reason_code == 0:
            self._conectado.set()

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: Any = None,
    ) -> None:
        self._conectado.clear()

    def conectar(self, timeout_s: float = 5.0) -> bool:
        """Estabelece a conexão TCP com o broker Mosquitto e inicia a thread de rede."""
        try:
            self.client.connect(self.broker_host, self.broker_port, keepalive=60)
            self.client.loop_start()
            return self._conectado.wait(timeout=timeout_s)
        except Exception:
            return False

    def enviar_leitura(self, payload_json_bytes: bytes) -> tuple[bool, str]:
        """
        Publica a leitura no tópico do sensor (RN01).
        RN27 e RN30: Se a conexão tiver caído, não enfileira para reenvio posterior da aplicação;
        registra falha de envio (contada como enviada e perdida) enquanto o loop tenta reconectar.
        """
        if not self._conectado.is_set() or not self.client.is_connected():
            return False, "sem_conexao"

        try:
            info = self.client.publish(self.topico, payload=payload_json_bytes, qos=self.qos)
            if info.rc == mqtt.MQTT_ERR_SUCCESS:
                return True, "ok"
            return False, f"mqtt_rc_{info.rc}"
        except Exception as exc:
            return False, f"erro_mqtt:{type(exc).__name__}"

    def fechar(self) -> None:
        """Para a thread de rede e desconecta o sensor do broker."""
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass

