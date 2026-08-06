Aqui está a tradução completa do documento para o Português do Brasil, mantendo os termos técnicos em inglês conforme solicitado.

***

# Medallion-RAG: Pipeline de Vetores Orquestrado para Aprimoramento de Contexto de LLM

> Um pipeline de dados pronto para produção que implementa a Arquitetura **Medallion** (Bronze → Silver → Gold) para realizar **ingestion**, **chunking**, **embedding** e servir artigos da Wikipedia por meio de um **vector database** PostgreSQL, orquestrado pelo Apache Airflow 3.

---

## Sumário

- [Vídeo de Apresentação e Demo](#vídeo-de-apresentação-e-demo)
- [Requisitos do Sistema](#requisitos-do-sistema)
- [Quick Start](#quick-start)
- [Definição do Problema](#definição-do-problema)
- [Arquitetura do Projeto](#arquitetura-do-projeto)
  - [Imagem Customizada do Airflow & Estratégia de Ingestion do Pacote](#imagem-customizada-do-airflow--estratégia-de-ingestion-do-pacote)
  - [Por que Airflow (e não Prefect)?](#por-que-airflow-e-não-prefect)
- [Pacote Medallion-RAG](#pacote-medallion-rag)
- [Orquestração com Airflow](#orquestração-com-airflow)
- [Schema do Database](#schema-do-database)
- [Configuração](#configuração)
- [Demo & Uso](#demo--uso)
- [Desenvolvimento](#desenvolvimento)
- [Troubleshooting](#troubleshooting)

---
## Vídeo de Apresentação e Demo
<youtube url video>

## Requisitos do Sistema

O projeto foi desenvolvido e testado no seguinte ambiente:

| Componente | Versão |
|-----------|---------|
| OS | WSL Ubuntu (x86_64) |
| Docker | 28.0.4 |
| Docker Compose | v2.34.0-desktop.1 |
| UV | v0.11.3 (x86_64-unknown-linux-gnu) |
| Python | ≥ 3.14 |

> **Nota:** O **pipeline** é totalmente containerizado. Desde que o Docker e o Docker Compose estejam disponíveis, o projeto deve rodar em qualquer host compatível (Linux, macOS, Windows com WSL2).

### Recomendações de Hardware

- **Recomendado:** ~10 GB RAM, 4 CPUs, 25 GB de disco

---

## Quick Start

### 1. Instalar UV 
`uv` será necessário para facilitar a demo. Instale `uv` em seu sistema com curl:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Clonar o Repositório

```bash
git clone https://github.com/vitormbesen/medallion-rag/tree/main
cd medallion-rag
```

### 3. Criar o Arquivo .env

Execute este único comando para gerar o `docker/.env` com o ID do usuário do seu host (evita problemas de permissão de volume no Linux/macOS):

```bash
printf "AIRFLOW_UID=%s\nAIRFLOW_PROJ_DIR=../packages/airflow\n" "$(id -u)" > docker/.env
```

### 4. Build e Iniciar os Serviços

```bash
cd docker
docker compose up --build -d
```

Isso irá:
- Construir uma imagem customizada do Airflow com o pacote `medallion-rag` instalado
- Iniciar o PostgreSQL (metadata **database** do Airflow)
- Iniciar o `pgvector/pgvector:pg16` (**vector database** do projeto na porta `5433`)
- Inicializar o **database** do Airflow e criar o usuário admin
- Iniciar o Airflow **API** Server (porta `8080`), **Scheduler** e o **DAG** Processor

A build completa leva cerca de 250 segundos.

### 5. Acessar a UI do Airflow

Abra o seu navegador em: **http://localhost:8080**

- **Username:** `airflow`
- **Password:** `airflow`

### 6. Executar o Pipeline

1. Navegue até **DAGs** → `rag_population`
2. Despause a **DAG** (chave alternadora)
3. (Opcional) Dispare uma execução manual ou aguarde a execução agendada

### 7. Consultar o Vector Database

Assim que o **pipeline** for concluído, você poderá consultar os **embeddings**:

```bash
cd demo
uv run python demo.py --query "Buddhism" --top-k 5
```

Ou utilize o Jupyter notebook:

```bash
uv run jupyter notebook demo/notebook.ipynb
```

---

## Definição do Problema

### Contexto

**Large Language Models (LLMs)** frequentemente sofrem com alucinações e conhecimento desatualizado ao responder a perguntas de domínios específicos. O **RAG (Retrieval Augmented Generation)** resolve isso fundamentando as respostas do LLM em uma base de conhecimento externa e curada.

### O Desafio

Construir um sistema de **RAG (Retrieval Augmented Generation)** envolve a coordenação de múltiplas etapas complexas:

1. **Ingestion:** Coletar documentos brutos (seja **APIs**, arquivos, **databases**, etc)
2. **Chunking:** Limpar, normalizar e fazer o **chunking** dos documentos em pedaços semanticamente significativos
3. **Embedding:** Converter **chunks** de texto em representações vetoriais de alta dimensão por meio the redes neurais
4. **Armazenamento:** Persistir vetores em um **database** otimizado para *Approximate Nearest Neighbor* (ANN) search
5. **Servimento (Serving):** Permitir a recuperação semântica de baixa latência para augment dos prompts de LLMs

Cada etapa possui modos de falha, requisitos de recursos e dependências das etapas anteriores, se tornando necessário um orquestração para garantir coordenação, reprodutibilidade e atualização do sistema.

---

## Arquitetura do Projeto

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              HOST MACHINE                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        Docker Compose Network                       │    │
│  │                                                                     │    │
│  │   ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │    │
│  │   │   Airflow    │    │   Airflow    │    │    Airflow DAG       │  │    │
│  │   │  API Server  │    │  Scheduler   │    │   Processor          │  │    │
│  │   │   :8080      │    │              │    │                      │  │    │
│  │   └──────┬───────┘    └──────┬───────┘    └──────────┬───────────┘  │    │
│  │          │                   │                       │              │    │
│  │          └───────────────────┼───────────────────────┘              │    │
│  │                              │                                      │    │
│  │                   ┌──────────▼──────────┐                           │    │
│  │                   │   PostgreSQL        │                           │    │
│  │                   │   (Airflow MetaDB)  │                           │    │
│  │                   │   :5432             │                           │    │
│  │                   └─────────────────────┘                           │    │
│  │                              │                                      │    │
│  │                   ┌──────────▼──────────┐                           │    │
│  │                   │   pgvector /        │                           │    │
│  │                   │   PostgreSQL 16     │◄──── localhost:5433       │    │
│  │                   │   (Project DB)      │                           │    │
│  │                   └─────────────────────┘                           │    │
│  │                              ▲                                      │    │
│  │                              │                                      │    │
│  │   ┌──────────────────────────┴──────────────────────────┐           │    │
│  │   │              medallion-rag (Python Package)          │          │    │
│  │   │  ┌─────────┐  ┌─────────┐  ┌─────────────────────┐  │           │    │
│  │   │  │ Bronze  │→ │ Silver  │→ │        Gold         │  │           │    │
│  │   │  │  API    │  │ Chunking│  │  Embedding (HNSW)   │  │           │    │
│  │   │  └─────────┘  └─────────┘  └─────────────────────┘  │           │    │
│  │   └─────────────────────────────────────────────────────┘           │    │
│  │                                                                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                         UV Workspace                                │   │
│   │  ┌─────────────────┐        ┌─────────────────┐                     │   │
│   │  │  packages/      │        │  demo/          │                     │   │
│   │  │  ├── airflow/   │        │  ├── demo.py    │                     │   │
│   │  │  └── medallion- │        │  └── notebook.  │                     │   │
│   │  │      rag/       │        │      ipynb      │                     │   │
│   │  └─────────────────┘        └─────────────────┘                     │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Imagem Customizada do Airflow com Source Code Instalado

Para executar as transformações de dados de forma isolada e performática dentro das **tasks** do Airflow, o pacote `medallion-rag` é **pré-instalado diretamente na imagem Docker customizada do Airflow** durante o **build** da imagem. 

De modo geral, pacotes de lógica de negócio possuem ciclos de vida independentes das ferramentas de orquestração que os executam. Por exemplo: o `medallion-rag` pode ser atualizado caso surja uma biblioteca mais eficiente de **tokenizer** ou **embedder**, ou se a **API** na etapa de **ingestion** alterar seus contratos. Por outro lado, o código das **DAGs** do Airflow evolui atrelado a componentes de infraestrutura, como **database**, **scheduler**, conexões e estratégias de scheduling. 

Dessa forma, o `medallion-rag` foi construído como um pacote Python à parte, o que permite manter a estrutura das **DAGs** enxutas (*"thin DAGs"*) e focadas exclusivamente na orquestração.

#### Como Funciona

1. **Resolução de Dependências do Workspace:** O pacote `packages/airflow` declara explicitamente o `medallion-rag` como uma dependência de caminho local do workspace no seu `pyproject.toml`:
   ```toml
   [project]
   name = "airflow"
   dependencies = [
       "apache-airflow[postgres,standard,fab]>=3.2.2",
       "medallion-rag>=0.1.0",
   ]

   [tool.uv.sources]
   medallion-rag = { path = "../medallion-rag" }
   ```

2. **Instalação no Build do Docker:** O `docker/Dockerfile` copia todo o diretório `packages/` para o contexto de build e usa `uv pip install --system` para compilar e instalar ambos os pacotes no ambiente Python do sistema (`/usr/local/bin/python`):
   ```dockerfile
   # Copia dependências dos pacotes medallion-rag + airflow
   COPY packages /opt/packages

   # Instala no ambiente Python do sistema
   RUN --mount=type=cache,target=/root/.cache/uv \
       uv pip install --system /opt/packages/airflow
   ```

3. **Uniformidade do Container:** A imagem Docker resultante é compartilhada entre todos os serviços do Airflow (`airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor` e contextos de worker da CLI) via Docker Compose (`x-airflow-common`). 

---

### Escolha do Airflow como orquestrador

| Critério | Airflow | Prefect |
|-----------|---------|---------|
| **Maturidade** | Testado em batalha desde 2015; ampla adoção corporativa | Moderno; mais simples para **workflows** em Python puro |
| **UI & Observabilidade** | UI web rica com gráficos de **DAG**, gráficos de Gantt, logs de **tasks** | UI limpa; menor visibilidade operacional |
| **Agendamento** | Agendamento robusto baseado em cron com suporte a backfill | Baseado em eventos e intervalos |
| **Ecossistema** | Enorme ecossistema de providers (Postgres, HTTP, etc.) | Crescente, porém menor |

**Decisão:** Apesar de a maioria dos sistemas maduros em nível empresarial ainda utilizar o Airflow 2.x, como o Airflow 3.x traz mudanças na topologia, escolhemos o **Apache Airflow 3.2.2** por ser a versão mais atualizada. Para esta **task** em particular, previmos que o processamento seria em lote (batch), portanto **orientado a agendamento** em vez de orientado a eventos. A maturidade do Airflow, os mecanismos robustos de **retry** e a rica interface de observabilidade o tornam ideal para **pipelines** de dados em produção, onde a confiabilidade e a auditabilidade são fundamentais.

---

## Pacote Medallion-RAG

A lógica principal reside em `packages/medallion-rag/`, um pacote Python que encapsula todo o processamento de dados independentemente do orquestrador.

### Estrutura do Pacote

```
packages/medallion-rag/
├── src/medallion_rag/
│   ├── persistence/
│   │   ├── models.py          # Models SQLAlchemy ORM (Bronze, Silver, Gold)
│   │   ├── reading.py         # Operações de leitura para transições de layer
│   │   ├── writing.py         # Operações de escrita com idempotência via UPSERT
│   │   └── __init__.py
│   ├── processing/
│   │   ├── bronze.py          # Ingestion via API da Wikipedia
│   │   ├── silver.py          # Chunking recursivo de texto
│   │   ├── gold.py            # Geração de embeddings com SentenceTransformer
│   │   └── __init__.py
│   ├── pipeline.py            # Funções de alta abstração para orquestração das layers
│   ├── search.py              # ANN search sobre a Gold layer
│   └── __init__.py
├── pyproject.toml
└── README.md
```

### Padrões de Design & Decisões

#### 1. Estratégia de Idempotência: UPSERT na Primary Key

Cada **layer** utiliza o comando `ON CONFLICT` do PostgreSQL para garantir a idempotência. 

- **Bronze:** **Upsert** na `document_id` (hash SHA-256 do conteúdo do documento)
- **Silver:** **Upsert** na `chunk_id` (hash SHA-256 de `document_id:chunk_idx:chunk_text`)
- **Gold:** Inserir-ou-ignorar na `chunk_id` (ignora **chunks** que já foram processados para **embedding**)

Isso significa que reexecutar o **pipeline** para a mesma `logical_date` nunca criará duplicatas: entradas são sempre reescritas quando há conflito.

#### 2. Único Database, Múltiplos Schemas

Em vez de implantar um **vector database** separado (ex: Pinecone, Weaviate, Milvus), utilizamos o **PostgreSQL com pgvector**:

- As **layers** **Bronze**, **Silver** e **Gold** coexistem como **schemas** em um único **database**, cada schema contendo sua própria **table**.
- Para **APIs** de inferência em produção, os vetores poderiam ser materializados em uma Feature Store (Feast com Milvus, por exemplo)

#### 3. SQLAlchemy ORM

Todas as definições de **schema** e queries (tanto para leitura quanto para escrita) usam o python ORM SQLAlchemy em vez de strings para queries do SQL. Isso garante que, durante o processo de desenvolvimento, possamos checar os tipos de cada colunas e gozar do **auto-completing** das IDEs.


#### 4. Geração de Embedding

Utilizamos o **embedding model** `sentence-transformers/all-MiniLM-L6-v2`: modelo simples, com dimensão de 384, tamanho leve ~90 MB para execução local no Airflow via biblioteca **SentenceTransformers**. A biblioteca abstrai os processo de instanciação do modelo, configuração, tokenization, batching, e manipulação de **tensors**. 


#### 5. Processamento em Batches

A **Gold layer** processa os **embeddings** em **batches** configuráveis (default `batch_size=32`) para gerenciar o uso de memória. Cada batch processada é escrita no database.

#### 6. Índice HNSW para ANN search

A **Gold layer** cria um índice HNSW (Hierarchical Navigable Small World), com `ip` (**Inner Product**) como cálculo da distância entre vetores, matematicamente equivalente ao **cosine similarity**.

```sql
CREATE INDEX idx_doc_embeddings_hnsw
ON gold.document_embeddings
USING hnsw (embedding vector_ip_ops)
WITH (m = 16, ef_construction = 64);
```

#### 7. Bronze Intercambiável

Neste projeto, a **Bronze layer** faz uma ingestão a partir da **Wikipedia Open API**. A função `fetch_document_from_api()` pode ser substituída, desde que acompanhada com alterações adequadas em outras partes do código para servir diferentes propósitos:
- Conectores CDC de **database**
- Watchers de sistema de arquivos
- **APIs** de CMS corporativos
- Feeds RSS ou web scrapers

---

## Orquestração com Airflow

A **layer** de orquestração reside em `packages/airflow/` e define a **DAG** `rag_population`.

### Estrutura da DAG

```
┌─────────────────────────────────────────────────────────────────┐
│                    rag_population DAG                           │
│                                                                 │
│   ┌─────────────┐                                               │
│   │init_database│  (cria schemas & tables se não existirem)     │
│   └──────┬──────┘                                               │
│          │                                                      │
│          ▼                                                      │
│   ┌─────────────┐                                               │
│   │   bronze    │  (dynamic task mapping: uma task por título)  │
│   │  [Buddhism] │                                               │
│   │  [Hinduism] │                                               │
│   │[Christianity]                                               │
│   │  [Judaism]  │                                               │
│   │  [Islamism] │                                               │
│   └──────┬──────┘                                               │
│          │                                                      │
│          ▼                                                      │
│   ┌─────────────┐                                               │
│   │   silver    │  (task única: lê docs bronze, gera chunks)    │
│   └──────┬──────┘                                               │
│          │                                                      │
│          ▼                                                      │
│   ┌─────────────┐                                               │
│   │    gold     │  (task única: carrega model, gera embeddings) │
│   └─────────────┘                                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Decisões de Design das Tasks

#### 1. Uma Task por Layer

A primeira task apenas realização uma inicialização do database. Ela não é realmente necessária e pode ser executada por fora durante o processo de bootstrap da infraestrutura adjacente. De toda forma, o pipeline utiliza exatamente três tasks sequenciais principais. 

A dependência Bronze → Silver → Gold garante que o processamento da próxima layer nunca inicie antes da conclusão da anterior. A depender do objetivo final da DAG, esse requisito por ser essencial para garantir uma compreensão completa do LLM que consome esses chunks.

Por exemplo: se um chatbot de e-commerce for questionado sobre a política de devolução de produtos, mas a **Gold layer** tiver sido processada com dados parciais da **Bronze** (devido à falta de sincronização entre as **layers**), o LLM não terá a visão completa (*"whole picture"*) das regras — podendo omitir prazos de reembolso ou exceções críticas, o que resulta em respostas incompletas ou alucinações.

#### 2. Mapeamento Dinâmico de Tasks (Dynamic Task Mapping) para a Bronze

```python
bronze = bronze_layer.partial(user_agent=config['bronze']['user_agent']).expand(
    title=config['bronze']['titles']
)
```

- Cada título da Wikipedia gera uma **task** independente
- As **tasks** rodam em paralelo até o limite de `max_active_tis_per_dagrun=4`
- Se um título falhar, os outros podem continuar; o **retry** é aplicado individualmente por título

#### 3. Task Única para Silver e Gold

- **Silver:** Uma única **task** lê todos os documentos **Bronze** daquela `logical_date` e faz o **chunking**. Embora pudesse ser dividida em lotes ou paralelizada, uma única **task** simplifica os limites de transação e evita estados parciais na **Silver layer**.
- **Gold:** Uma única **task** carrega o **model** de **embedding** **uma só vez** e processa todos os **batches**. O carregamento do **model** é custoso e o mesmos já possuem o processamento em **batch** que pode ser aproveitado.

#### 4. Sem Uso de XCOM Entre Tasks

As **tasks** **não** passam dados via XCOM. Em vez disso, cada **task** lê e escreve diretamente no **database**, pois os textos dos documentos e os **embeddings** são volumosos, o que facilmente sobrecarregariam o backend do XCOM, além da duplicidade de dados (estariam persistidos tanto no database criado como no XCOM).


#### 5. Configurabilidade

O comportamento da **DAG** é controlado pelo arquivo `packages/airflow/config/medallion_rag_config.yaml`:

```yaml
bronze:
  user_agent: "MedallionRAG/1.0 (1664842@pucminas.edu.br) Airflow-class-project"
  titles:
    - Buddhism
    - Hinduism
    - Christianity
    - Judaism
    - Islamism

gold:
  model: 'sentence-transformers/all-MiniLM-L6-v2'
  batch_size: 32
```

Para adicionar um novo tópico, basta editar este arquivo, sem alterações de código, sem necessidade de reimplantação da **DAG**. Vale notar que, caso haja modificação do modelo de emebedding e da dimensão dos embeddings que o mesmo produz, será necessário atualizar algumas partes do código, bem como atualizar o database.

#### 6. Resiliência: Retries, Backoff e Callbacks

```python
task_common_args = dict(
    retries=3,
    retry_delay=pendulum.duration(minutes=5),
    retry_exponential_backoff=True,
    max_active_tis_per_dagrun=4,
    on_success_callback=_on_success_callback,
    on_retry_callback=_on_retry_callback,
    on_failure_callback=_on_failure_callback,
)
```

- **Retries:** Até 3 tentativas por **task** (trata falhas transitórias de **API** e locks no **database**)
- **Exponential Backoff:** Evita o efeito de sobrecarga no sistema durante a recuperação
- **Callbacks:** Aqui, para fins do projeto, eles apenas logam. Em amientes de produção, ideal é configurar alertas para Slack, Teams, e-mail, por exemplo.


#### 7. Gerenciamento do Ciclo de Vida (Lifecycle Management)

Cada **task** cria sua própria engine SQLAlchemy com `NullPool` e a encerra explicitamente dentro de um bloco `finally`, gerenciando o ciclo de vida das conexões com o **database**.

---

## Schema do Database

### Bronze Layer (`bronze.raw_documents`)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `document_id` | `VARCHAR(255) PK` | Hash SHA-256 do texto do documento |
| `payload` | `JSONB` | Resposta bruta da **API** (título, texto, URL, metadados) |
| `extracted_at` | `TIMESTAMPTZ` | Timestamp da **ingestion** |
| `logical_date` | `TIMESTAMPTZ` | Data de execução da **DAG** (chave de partição) |

### Silver Layer (`silver.processed_chunks`)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `chunk_id` | `VARCHAR(255) PK` | Hash SHA-256 de `doc_id:idx:text` |
| `document_id` | `VARCHAR(255) FK` | Referência ao documento na **Bronze** |
| `chunk_index` | `INTEGER` | Posição do **chunk** dentro do documento |
| `chunk_text` | `TEXT` | Conteúdo do **chunk** (≤500 chars com sobreposição) |
| `processed_at` | `TIMESTAMPTZ` | Timestamp do processamento |
| `logical_date` | `TIMESTAMPTZ` | Data de execução da **DAG** |

**Constraint:** `UNIQUE(document_id, chunk_index)`

### Gold Layer (`gold.document_embeddings`)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `chunk_id` | `VARCHAR(255) PK/FK` | Referência ao **chunk** na **Silver** |
| `document_id` | `VARCHAR(255)` | Desnormalizado para eficiência de consulta |
| `embedding` | `VECTOR(384)` | **Embedding** gerado pelo all-MiniLM-L6-v2 |
| `updated_at` | `TIMESTAMPTZ` | Timestamp da geração do **embedding** |
| `logical_date` | `TIMESTAMPTZ` | Data de execução da **DAG** |

**Índice:** `idx_doc_embeddings_hnsw` (HNSW na coluna `embedding` com `vector_ip_ops`)

---

## Configuração

### Variáveis de Ambiente

| Variável | Padrão | Descrição |
|----------|---------|-----------|
| `AIRFLOW_UID` | `50000` | ID de usuário para os containers do Airflow |
| `AIRFLOW_PROJ_DIR` | `.` | Caminho base para montagem de volumes |


### Customizando Tópicos

Edite o arquivo `packages/airflow/config/medallion_rag_config.yaml` e reinicie o **DAG** processor (ou aguarde o próximo intervalo de parse):

```yaml
bronze:
  titles:
    - Buddhism
    - Hinduism
    - Christianity
    - Judaism
    - Islamism
    - Sikhism        # ← adicione novos tópicos aqui
    - Taoism
```

### Ajustando a Busca ANN

A CLI do `demo.py` suporta o ajuste de taxa de recuperação vs. velocidade:

```bash
uv run python demo.py --query "meditation" --top-k 10 --ef-search 100
```

- `--ef-search 40`: Rápido, taxa de recuperação moderada (padrão)
- `--ef-search 100`: Mais lento, maior taxa de recuperação
- `--ef-search 200`: Melhor taxa de recuperação, significativamente mais lento

---

## Demo & Uso

### Busca via CLI

```bash
cd demo
uv run python demo.py --query "India" --top-k 5 --explain
```

**Exemplo de Output:**

```
gold.document_embeddings: 930 rows

--- EXPLAIN ANALYZE ---
Limit  (cost=..)
  ->  Index Scan using idx_doc_embeddings_hnsw on document_embeddings
        Order By: (embedding <#> '[...]'::vector)
-----------------------

[1] similarity=0.5098  doc_id=1dd96f25ae…
United Kingdom, the United States, and other western countries)
--------------------------------------------------------------------------------
[2] similarity=0.3986  doc_id=fa85eca4d9…
==== Modern India and the world ==== The scope of Hinduism is increasing...
--------------------------------------------------------------------------------
```

### Jupyter Notebook

```bash
cd demo
uv sync
```

Abra o notebook. Selecione o kernel local. O notebook oferece um ambiente interativo para explorar consultas e visualizar os scores de similaridade.

---

## Desenvolvimento

### UV Workspace

O projeto utiliza workspaces do UV para gerenciar pacotes interdependentes:

```
packages/
├── pyproject.toml          # Raiz do Workspace
├── medallion-rag/          # Biblioteca Core
│   └── pyproject.toml
└── airflow/                # Layer de Orquestração
    └── pyproject.toml
```

## Declaração de Uso de IA

Este **README** foi elaborado com o auxílio de **LLMs** para organização, estruturação, criação de diagramas, e descrição dos componentes técnicos, sendo integralmente revisado, ajustado e validado pelo autor.