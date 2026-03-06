# Medicare Appeals Analytics

End-to-end Databricks solution for Medicare appeals analysis: synthetic data generation, a Lakeflow (DLT) medallion pipeline, a Multi-Agent Supervisor (MAS) chatbot, LLM-as-judge evaluation, and a deployed Databricks App with a React frontend.

## Architecture

```
data_generation/          Faker-based PySpark notebook (6 tables)
        │
        ▼
pipeline/                 Lakeflow DLT (bronze → silver → gold)
  01_bronze.sql
  02_silver.sql
  03_gold.sql
        │
        ▼
MAS Agent (Databricks)    Multi-Agent Supervisor + Genie + MCP tool
        │
        ▼
app/                      Databricks App (FastAPI + React)
  server/                   API routes (chat, summary)
  frontend/                 React + Tailwind chat UI
        │
        ▼
agent/                    MLflow LLM judges (5 scorers)
  register_judges.py
```

## Multi-Agent Supervisor (MAS) Architecture

```
                         ┌──────────────────────────┐
                         │     User (Chat UI)        │
                         └────────────┬───────────────┘
                                      │  POST /api/chat
                                      ▼
                         ┌──────────────────────────┐
                         │   Databricks App          │
                         │   (FastAPI + React)       │
                         └────────────┬───────────────┘
                                      │  /serving-endpoints/.../invocations
                                      ▼
                ┌─────────────────────────────────────────────┐
                │          MAS — Multi-Agent Supervisor        │
                │  (Foundation Model orchestrates tool calls)  │
                └──┬──────────────┬──────────────┬────────────┘
                   │              │              │
          ┌────────▼───────┐ ┌───▼────────┐ ┌───▼──────────────┐
          │  Genie Space   │ │ DBSQL Fns  │ │  MCP Tool        │
          │  (NL → SQL)    │ │ (direct    │ │  (Part D data)   │
          │                │ │  queries)  │ │                  │
          │  Gold-layer    │ │  Claims    │ │  mcp-partd       │
          │  tables from   │ │  Appeals   │ │  .medseal.app    │
          │  DLT pipeline  │ │  Members   │ │                  │
          │                │ │  Eligib.   │ │  Formulary,      │
          │  Appeals stats │ │  PAs       │ │  pricing,        │
          │  Denial trends │ │            │ │  coverage data   │
          │  Overturn rates│ │            │ │  (GLP-1 drugs)   │
          └────────┬───────┘ └───┬────────┘ └───┬──────────────┘
                   │             │              │
                   ▼             ▼              ▼
          ┌──────────────────────────────────────────────────┐
          │              Unity Catalog                        │
          │   catalog.schema.bronze_*  / silver_*  / gold_*  │
          └──────────────────────────────────────────────────┘
```

**Flow**: The supervisor receives a user message, decides which tool(s) to invoke (Genie for analytical questions, DBSQL functions for member/claim lookups, MCP for external drug data), aggregates the results, and returns a unified response. All tool calls are traced via MLflow, where 5 LLM judges continuously evaluate quality.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI (`>= 0.210`) configured with a profile
- Python 3.10+
- Node.js 18+ (only if modifying the React frontend)

## Quick Start

### 1. Clone and configure

```bash
git clone <repo-url>
cd appeals-analytics
```

Edit the following variables for your environment:

| File | Variable | Description |
|---|---|---|
| `data_generation/generate_data.py` | `CATALOG`, `SCHEMA` | Target Unity Catalog location |
| `databricks.yml` | `variables.catalog`, `variables.schema` | Same as above (for DABs) |
| `agent/register_judges.py` | `EXPERIMENT_ID` | Your MLflow experiment ID |
| `app/app.yaml` | `MAS_ENDPOINT_NAME` | Your MAS serving endpoint |

### 2. Generate synthetic data

Import `data_generation/generate_data.py` as a Databricks notebook and run all cells. This creates 6 tables (~1,450 rows total) in your catalog/schema.

### 3. Deploy the DLT pipeline

```bash
databricks bundle deploy -p <your-profile>
databricks bundle run appeals_analytics_pipeline -p <your-profile>
```

This creates bronze/silver/gold views and tables via Lakeflow Declarative Pipelines.

### 4. Create a Genie Space

In the Databricks UI:
1. Go to **Genie** and create a new space
2. Add the gold-layer tables from your schema
3. Add general instructions describing the Medicare appeals domain
4. Note the Genie Space ID for the MAS agent configuration

### 5. Set up the MAS Agent

In the Databricks UI:
1. Go to **Playground → Multi-Agent Supervisor**
2. Add a **Genie** tool pointing to your Genie Space
3. Add an **MCP** tool with endpoint: `https://mcp-partd.medseal.app/mcp`
4. Review and deploy the agent to a serving endpoint
5. Note the endpoint name (used in `app/app.yaml`)

### 6. Deploy the app

```bash
# Using REST API (recommended for CLI < 0.229)
# See Databricks Apps REST API docs

# Or with newer CLI:
databricks apps create --name medicare-appeals-chat --json '{"description": "Medicare Appeals Chat"}'
databricks apps deploy medicare-appeals-chat --source-code-path app/
```

Grant the app's service principal `CAN_QUERY` permission on the MAS endpoint.

### 7. Register LLM judges

Import `agent/register_judges.py` as a Databricks notebook and run it on a cluster with:
```
pip install --upgrade mlflow databricks-agents
```

This registers 5 LLM-as-judge scorers on your MLflow experiment for live traffic monitoring.

### 8. (Optional) Build the frontend

Only needed if you modify the React source:

```bash
cd app/frontend
npm install
npm run build
```

The built assets in `dist/` are served by FastAPI.

## Configuration Reference

| Environment Variable | Used In | Default | Description |
|---|---|---|---|
| `WAREHOUSE_ID` | `app/server/routes/summary.py` | `4b28691c780d9875` | SQL warehouse for dashboard queries |
| `CATALOG_SCHEMA` | `app/server/routes/summary.py` | `hls_amer_catalog.rmukherjee` | `catalog.schema` for SQL queries |
| `DATABRICKS_PROFILE` | `app/server/config.py` | `hls_amer` | CLI profile for local development |
| `MAS_ENDPOINT_NAME` | `app/app.yaml` | `mas-c69b95e4-endpoint` | MAS serving endpoint name |

## LLM Judges

| Scorer | Type | What it evaluates |
|---|---|---|
| `safety` | Built-in Safety | Response safety and appropriateness |
| `professional_tone` | Guidelines (3 rules) | Healthcare-appropriate communication |
| `response_completeness` | Guidelines (3 rules) | Actionable, data-backed answers |
| `routing_quality` | Custom (3-level) | Correct tool routing by supervisor |
| `appeals_domain_accuracy` | Custom (3-level) | Medicare appeals domain correctness |

## MCP Connection

The MAS agent connects to a Medicare Part D external data source via MCP:
- **Endpoint**: `https://mcp-partd.medseal.app/mcp`
- Provides formulary, pricing, and coverage data for GLP-1 and other drugs

## Project Structure

```
appeals-analytics/
├── app/                          Databricks App
│   ├── app.py                    FastAPI entrypoint
│   ├── app.yaml                  App configuration
│   ├── requirements.txt          Python dependencies
│   ├── server/
│   │   ├── config.py             Auth (SPN / local profile)
│   │   └── routes/
│   │       ├── chat.py           POST /api/chat → MAS endpoint
│   │       └── summary.py        GET /api/summary → SQL warehouse
│   └── frontend/                 React + Tailwind source
├── agent/
│   └── register_judges.py        5 LLM-as-judge scorers
├── data_generation/
│   └── generate_data.py          Synthetic data notebook
├── pipeline/
│   ├── 01_bronze.sql             Raw table ingestion
│   ├── 02_silver.sql             Cleaned / typed tables
│   └── 03_gold.sql               Aggregated analytics views
├── resources/
│   └── appeals_pipeline.yml      DLT pipeline resource config
└── databricks.yml                DAB bundle definition
```
