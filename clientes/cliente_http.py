"""
Cliente HTTP por sensor (RN27 a RN30).
- Cada sensor simulado possui sua própria requests.Session() com TCP keep-alive (RN29)
- Nenhuma retransmissão na camada de aplicação: HTTPAdapter(max_retries=0) (RN27)
- Timeout de 5 segundos por requisição (RN28)
"""

import requests
from requests.adapters import HTTPAdapter

from comum.modelo import TIMEOUT_HTTP_S


class ClienteHTTPSensor:
    """Mantém uma conexão HTTP persistente exclusiva para um único sensor (RN29)."""

    def __init__(self, sensor_id: str, url_base: str = "http://127.0.0.1:8000") -> None:
        self.sensor_id = sensor_id
        self.url_leituras = f"{url_base.rstrip('/')}/leituras"
        self.timeout = TIMEOUT_HTTP_S

        # Cria uma sessão HTTP exclusiva para este sensor (1 sensor = 1 conexão TCP persistente)
        self.session = requests.Session()
        # RN27: max_retries=0 garante que a aplicação não reenvie em caso de falha
        adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1, max_retries=0)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Connection": "keep-alive",
                "User-Agent": f"DHT22-Sim/{sensor_id}",
            }
        )

    def conectar(self) -> bool:
        """
        Pré-aquece a conexão TCP persistente da sessão consultando /saude antes da rodada,
        para que os 100 sockets TCP já estejam abertos como no MQTT.
        """
        url_saude = self.url_leituras.rsplit("/", 1)[0] + "/saude"
        try:
            resp = self.session.get(url_saude, timeout=self.timeout)
            return resp.status_code == 200
        except requests.RequestException:
            return False

    def enviar_leitura(self, payload_json_bytes: bytes) -> tuple[bool, str]:
        """
        Envia o payload JSON para POST /leituras com timeout de 5 s (RN28).
        Se falhar, não reenvia (RN27, RN28) e retorna (False, motivo).
        Na leitura seguinte, a requests.Session tentará reconectar automaticamente (RN30).
        """
        try:
            resp = self.session.post(
                self.url_leituras,
                data=payload_json_bytes,
                timeout=self.timeout,
            )
            if resp.status_code == 204:
                return True, "ok"
            return False, f"http_{resp.status_code}"
        except requests.Timeout:
            return False, "timeout"
        except requests.RequestException as exc:
            return False, f"erro_conexao:{type(exc).__name__}"

    def fechar(self) -> None:
        """Encerra a sessão HTTP e fecha o socket TCP do sensor."""
        self.session.close()

