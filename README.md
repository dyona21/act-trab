# Comparação Experimental: HTTP/REST vs. MQTT (QoS 0 e QoS 1) no Monitoramento de Sensores

Este projeto implementa um ambiente experimental controlado para comparar o desempenho dos protocolos **HTTP/REST (FastAPI/Uvicorn)** e **MQTT (Eclipse Mosquitto com QoS 0 e QoS 1)** no monitoramento contínuo de temperatura e umidade de um laboratório refrigerado de reagentes com sensores **DHT22** simulados (`10`, `50` e `100` sensores).

---

## 1. O que o Projeto Faz?

O sistema simula até **100 sensores DHT22** (`sensor01` a `sensor100`) instalados na `sala1`, enviando leituras a cada **5 segundos** sem deriva de tempo.

Para garantir uma **comparação científica justa** onde a única diferença medida seja o protocolo de rede:
- **Mesmo código de negócio (`comum/`)**: Tanto a API HTTP quanto o Subscriber MQTT importam e chamam exatamente a mesma função (`ProcessadorLeituras.processar_payload`) para validar o JSON, filtrar duplicatas, abrir/fechar alertas e armazenar os registros.
- **Mesmo payload JSON**: `{"sensor_id": "sensor01", "seq": 0, "t_envio": ..., "temperatura": 5.0, "umidade": 45.0}`.
- **Mesmo relógio monotônico (`time.perf_counter_ns()`)**: Todas as marcações de tempo (`t_envio`, `t_recebimento`, `t_alerta` e janelas da rodada) utilizam o contador de alta resolução compartilhado do sistema operacional (`QueryPerformanceCounter` no Windows), evitando distorções de relógio entre processos.
- **Zero escrita em disco durante a rodada**: As leituras e os alertas ficam em buffers na memória RAM durante toda a execução e só são gravados em arquivos CSV no final da rodada (`POST /finalizar` no HTTP ou tópico `lab/controle/finalizar` / encerramento no MQTT).
- **1 conexão TCP persistente por sensor**: No HTTP, cada sensor possui sua própria `requests.Session()` (`Keep-Alive`) e o Uvicorn roda com `--timeout-keep-alive 60`. No MQTT, cada sensor mantém seu próprio `mqtt.Client` conectado ao broker.
- **Configuração equivalente dos servidores**: Processo único (`1 worker` no Uvicorn, processo único no Mosquitto), sem TLS, sem autenticação e com logs de acesso desligados no caminho crítico.

---

## 2. Estrutura do Projeto

```text
projeto/
├── comum/
│   ├── modelo.py            # Modelo Pydantic (Leitura) e constantes das Regras de Negócio (RN01-RN49)
│   └── processamento.py     # Buffer em RAM, controle de duplicatas e máquina de estados de alertas
├── servidores/
│   ├── api_http.py          # API FastAPI (POST /leituras -> 204, GET /saude, POST /finalizar)
│   └── subscriber_mqtt.py   # Subscriber MQTT (paho-mqtt 2.x) que processa as leituras do broker
├── clientes/
│   ├── sensor.py            # Simulador de leituras DHT22 e gerador determinístico de excursões
│   ├── cliente_http.py      # Cliente HTTP persistente por sensor (requests.Session, timeout=5s, retries=0)
│   ├── cliente_mqtt.py      # Cliente MQTT persistente por sensor (paho-mqtt 2.x, QoS 0 ou 1)
│   └── executar_rodada.py   # Orquestrador de rodadas (10/50/100 sensores) e gerador do plano sorteado
├── analise/
│   └── analisar.py          # Cálculo de métricas (latência, entrega, duplicatas, tráfego) e gráficos
├── dados/                   # Diretório onde são salvos os CSVs, metadados JSON e gráficos PNG
├── mosquitto.conf           # Configuração enxuta do broker Mosquitto (sem logs, sem persistência)
└── requirements.txt         # Versões congeladas das bibliotecas Python utilizadas
```

---

## 3. Resumo das Regras de Negócio Implementadas

| Grupo | Regras Principais |
| :--- | :--- |
| **Sensores e Leituras (`RN01`–`RN08`)** | Identificadores `sensor01` a `sensor100`; tópico MQTT `lab/sala1/<sensor_id>/dados`; envio a cada `5 s` sem deriva; defasagem inicial aleatória entre `0` e `5 s` (mesma semente entre protocolos); condição normal em torno de `5 °C` e `45%` de umidade; faixa física do DHT22 de `-40` a `80 °C` e `0` a `100%`. |
| **Limiares e Alertas (`RN09`–`RN15`)** | Faixa normal inclusiva: `2 °C <= temperatura <= 8 °C` e `30% <= umidade <= 60%`. O alerta abre na **primeira** leitura em violação, permanece aberto em leituras consecutivas em violação (sem duplicar alertas) e fecha quando uma leitura volta à faixa normal. |
| **Excursões Programadas (`RN16`–`RN19`)** | `10%` dos sensores sofrem excursões a cada `120 s` com duração de `30 s` (`9` a `12 °C` ou `65` a `75%` de umidade), proibidas nos `30 s` iniciais de aquecimento e nos `10 s` finais. |
| **Recepção e Contrato (`RN20`–`RN26`)** | `t_recebimento` marcado como 1º passo do handler; JSON malformado, tipo incorreto ou valor fora da faixa física do DHT22 retorna `400 Bad Request` na API e é descartado no subscriber (contado em `total_invalidas`); sucesso retorna `204 No Content` sem corpo. |
| **Experimento e Métricas (`RN32`–`RN49`)** | 9 cenários (`{MQTT QoS 0, MQTT QoS 1, HTTP}` $\times$ `{10, 50, 100 sensores}`), 5 repetições por cenário (`30 s` de aquecimento excluídos das métricas + `600 s` de medição + `10 s` de tolerância final). Critérios de decisão: entrega $\ge 99\%$ com 100 sensores (`RN46`) e tempo de alerta `p99 <= 5 s` (`RN47`). |

---

## 4. Como Preparar o Ambiente

1. **Abra o terminal na raiz do projeto** (`Ctrl + '` no VS Code / Antigravity).
2. **Ative o ambiente virtual (`.venv`)**:
   ```powershell
   .\.venv\Scripts\activate
   ```
   *(Caso o PowerShell bloqueie a execução de scripts, rode antes: `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`)*
3. **Caso precise reinstalar as dependências em outra máquina**:
   ```powershell
   pip install -r requirements.txt
   ```

---

## 5. Como Rodar o Projeto

### Opção A: Executar uma Rodada HTTP / REST

Você precisará de **2 terminais** abertos na raiz do projeto (ambos com `.\.venv\Scripts\activate` ativo):

**Terminal 1 — Subir a API HTTP (Uvicorn):**
```powershell
python -m servidores.api_http --host 0.0.0.0 --port 8000 --timeout-keep-alive 60
```
*(Ou diretamente via comando do Uvicorn: `uvicorn servidores.api_http:app --host 0.0.0.0 --port 8000 --workers 1 --no-access-log --timeout-keep-alive 60`)*

**Terminal 2 — Executar o gerador de carga (Sensores HTTP):**
- Para um **teste rápido de validação** (ex.: 5s de aquecimento + 15s de medição com 10 sensores):
  ```powershell
  python -m clientes.executar_rodada --protocolo http --sensores 10 --repeticao 1 --semente 10137 --aquecimento 5 --duracao 15 --tolerancia-fim 2
  ```
- Para uma **rodada oficial completa** (30s de aquecimento + 600s de medição + 10s de tolerância):
  ```powershell
  python -m clientes.executar_rodada --protocolo http --sensores 100 --repeticao 1 --semente 10137
  ```
Ao término da rodada, o próprio `executar_rodada.py` chama automaticamente o endpoint `POST /finalizar` da API e grava todos os CSVs e metadados na pasta `dados/`.

---

### Opção B: Executar uma Rodada MQTT (QoS 0 ou QoS 1)

Você precisará de **3 terminais** abertos na raiz do projeto:

**Terminal 1 — Subir o Broker Mosquitto:**
```powershell
mosquitto -c mosquitto.conf
```

**Terminal 2 — Subir o Subscriber MQTT (com `.venv` ativo):**
- Para **QoS 0**:
  ```powershell
  python -m servidores.subscriber_mqtt --host 127.0.0.1 --port 1883 --qos 0 --cenario mqtt_qos0_10s --repeticao 1
  ```
- Para **QoS 1**:
  ```powershell
  python -m servidores.subscriber_mqtt --host 127.0.0.1 --port 1883 --qos 1 --cenario mqtt_qos1_10s --repeticao 1
  ```

**Terminal 3 — Executar o gerador de carga (Sensores MQTT com `.venv` ativo):**
- Teste rápido (QoS 0, 10 sensores):
  ```powershell
  python -m clientes.executar_rodada --protocolo mqtt_qos0 --sensores 10 --repeticao 1 --semente 10137 --aquecimento 5 --duracao 15 --tolerancia-fim 2
  ```
- Rodada oficial (QoS 1, 100 sensores):
  ```powershell
  python -m clientes.executar_rodada --protocolo mqtt_qos1 --sensores 100 --repeticao 1 --semente 10137
  ```

---

### Opção C: Testar Manualmente as Rotas da API com `curl`

Com a API rodando no Terminal 1, você pode testar os contratos (`RN23`, `RN24`, `RN25`) em outro terminal:

```powershell
# 1. Verificar saúde e contadores em memória (esperado: 200 OK)
curl.exe -i http://127.0.0.1:8000/saude

# 2. Enviar leitura normal válida usando arquivo JSON (esperado: 204 No Content)
curl.exe -i -X POST http://127.0.0.1:8000/leituras -H "Content-Type: application/json" --data-binary "@ok.json"

# 3. Enviar leitura em violação de limiar (esperado: 204 No Content e abertura de 1 alerta)
curl.exe -i -X POST http://127.0.0.1:8000/leituras -H "Content-Type: application/json" --data-binary "@alerta.json"

# 4. Enviar leitura inválida ou fora da faixa física do DHT22 (esperado: 400 Bad Request)
curl.exe -i -X POST http://127.0.0.1:8000/leituras -H "Content-Type: application/json" --data-binary "@invalido.json"
curl.exe -i -X POST http://127.0.0.1:8000/leituras -H "Content-Type: application/json" --data-binary "@fisico.json"

# 5. Forçar gravação dos buffers em disco (esperado: 200 OK e geração dos CSVs em dados/)
curl.exe -i -X POST http://127.0.0.1:8000/finalizar -H "Content-Type: application/json" -d "{\"cenario_id\": \"teste_manual\", \"repeticao\": 1}"
```

---

## 6. Sorteio das Rodadas e Análise dos Resultados

1. **Gerar o plano sorteado das 45 rodadas (`RN32`, `RN35`)**:
   ```powershell
   python -m clientes.executar_rodada --gerar-plano
   ```
   Isso gera o arquivo `dados/plano_sorteado_45_rodadas.json` com a ordem aleatória de execução e as sementes padronizadas por repetição.

2. **Processar os CSVs, calcular as métricas (`RN40`–`RN45`), avaliar os critérios (`RN46`–`RN48`) e gerar gráficos**:
   ```powershell
   python -m analise.analisar --dados dados
   ```
   O script irá gerar na pasta `dados/`:
   - `metricas_por_rodada.csv`: todas as métricas individuais de cada rodada executada.
   - `metricas_agregadas_cenarios.csv`: média e desvio-padrão entre as repetições de cada cenário (`RN45`).
   - `decisao_rn46_rn48.json`: protocolo qualificado/vencedor conforme os critérios de confiabilidade, tempo de alerta, tráfego e latência `p95`.
   - `grafico_latencia_p95.png` e `grafico_taxa_entrega.png`: gráficos comparativos entre os protocolos.

