"""
API HTTP (FastAPI) para recepção das leituras dos sensores (RN20 a RN26).

Características para comparação justa com o Mosquitto:
- Mesmo módulo de validação e processamento (comum.processamento)
- Marca t_recebimento = time.perf_counter_ns() como primeiro passo do handler (RN21)
- Retorna 204 No Content sem corpo no caminho crítico (RN25) e 400 Bad Request para inválidas (RN23, RN24)
- Apenas as 3 rotas do contrato: POST /leituras, GET /saude e POST /finalizar (RN25)
- Execução via Uvicorn com 1 worker, --no-access-log e --timeout-keep-alive 60
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Optional
import uvicorn
from fastapi import FastAPI, Request, Response

# Garante que a raiz do projeto esteja no sys.path mesmo ao executar `python servidores/api_http.py`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from comum.modelo import FinalizarRequisicao
from comum.processamento import ProcessadorLeituras

# Desativa rotas automáticas de documentação (/docs, /redoc, /openapi.json) para cumprir RN25 ("Não há outras rotas")
app = FastAPI(
    title="API Monitoramento Laboratório (HTTP/REST)",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

processador = ProcessadorLeituras(protocolo="HTTP")


@app.post("/leituras", status_code=204, response_class=Response)
async def receber_leitura(request: Request) -> Response:
    """
    Caminho crítico HTTP (Seção 1 e RN20 a RN25):
    1. Marca o instante de recebimento (time.perf_counter_ns)
    2. Valida JSON e limites físicos do DHT22
    3. Registra leitura em memória e avalia limiares de alerta
    """
    t_recebimento = time.perf_counter_ns()
    payload_bruto = await request.body()

    valida = processador.processar_payload(payload_bruto, t_recebimento)
    if not valida:
        # RN23 e RN24: Mensagem inválida responde 400 sem corpo grande
        return Response(status_code=400)

    # RN25: Resposta mínima 204 sem corpo para não inflar o tráfego de rede
    return Response(status_code=204)


@app.get("/saude")
async def verificar_saude() -> dict:
    """
    Endpoint auxiliar fora do caminho crítico (RN25) para confirmar que a API subiu
    e consultar contadores em memória.
    """
    return {
        "status": "ativo",
        **processador.obter_estatisticas(),
    }


@app.post("/finalizar")
async def finalizar_rodada(req: Optional[FinalizarRequisicao] = None) -> dict:
    """
    Endpoint auxiliar fora do caminho crítico (RN25, RN26) que força a gravação
    dos buffers em memória para arquivos CSV no fim da rodada e limpa o estado.
    """
    parametros = req or FinalizarRequisicao()
    resumo = processador.gravar_em_disco(
        cenario_id=parametros.cenario_id or "http_padrao",
        repeticao=parametros.repeticao or 1,
        diretorio_saida=parametros.diretorio_saida or "dados",
        limpar_memoria=parametros.limpar_memoria,
    )
    return {"status": "finalizado", "resumo": resumo}


def iniciar_servidor(host: str = "0.0.0.0", port: int = 8000, keep_alive: int = 60) -> None:
    """
    Inicia o Uvicorn configurado de forma equivalente ao Mosquitto:
    - 1 único worker (processo único)
    - access_log=False (--no-access-log) para não distorcer a latência
    - timeout_keep_alive=60 para manter as 100 conexões TCP persistentes entre envios de 5 s
    """
    uvicorn.run(
        app,
        host=host,
        port=port,
        workers=1,
        access_log=False,
        timeout_keep_alive=keep_alive,
        log_level="warning",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Servidor API HTTP FastAPI/Uvicorn")
    parser.add_argument("--host", default="0.0.0.0", help="Endereço de bind (padrão: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Porta TCP (padrão: 8000)")
    parser.add_argument(
        "--timeout-keep-alive",
        type=int,
        default=60,
        help="Timeout de keep-alive em segundos (padrão: 60)",
    )
    args = parser.parse_args()
    iniciar_servidor(host=args.host, port=args.port, keep_alive=args.timeout_keep_alive)

