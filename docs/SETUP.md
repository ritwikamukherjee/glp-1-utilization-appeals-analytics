# Setup Guide

## Configuration

### Unity Catalog Configuration

The project uses a configurable catalog and schema. Edit `config.py` to set your preferences:

```python
CATALOG = "healthcare"  # Your catalog name
SCHEMA = "appeals_review"  # Your schema name
```

**You don't need to use `ai_dev_kit`** - that's just a default from the AI dev kit. Use any catalog/schema you prefer.

### Data Storage

Data is stored in Unity Catalog Volumes:
- Path: `/Volumes/{CATALOG}/{SCHEMA}/raw_data/`
- Format: Parquet files (one folder per table)

### Data Generation Settings

Edit `config.py` to adjust data volumes:

```python
N_MEMBERS = 5000
N_PROVIDERS = 500
N_PRIOR_AUTHS = 15000
N_CLAIMS = 30000
N_APPEALS = 2000
```

## Running Data Generation

### Option 1: Via Databricks MCP (Recommended)

If you have the Databricks MCP server configured:

1. Make sure `config.py` has your preferred catalog/schema
2. Execute the script using Databricks MCP tools
3. The script will:
   - Create catalog/schema/volume if they don't exist
   - Generate synthetic healthcare data
   - Save to Unity Catalog Volume

### Option 2: Direct Execution on Databricks

1. Upload `config.py` and `appeals-review/scripts/generate_healthcare_data.py` to your Databricks workspace
2. Run the script in a Databricks notebook or job
3. Ensure the cluster has `faker` and `holidays` installed:
   ```python
   %pip install faker holidays
   ```

## Project Organization

### Code Assets
- `appeals-review/scripts/` - Python scripts for data generation
- `config.py` - Centralized configuration (at project root)
- `appeals-review/docs/` - Documentation

### Generated Data
- Stored in Unity Catalog Volumes (not in this repo)
- Path: `/Volumes/{CATALOG}/{SCHEMA}/raw_data/`
- Format: Parquet files organized by table name

### Future Assets
- `pipelines/` - Spark Declarative Pipeline definitions
- `agents/` - Agent Brick configurations
- `notebooks/` - Databricks notebooks (if needed)

## Customizing Catalog/Schema

To use a different catalog and schema:

1. Edit `config.py`:
   ```python
   CATALOG = "your_catalog_name"
   SCHEMA = "your_schema_name"
   ```

2. The script will automatically:
   - Create the catalog if it doesn't exist
   - Create the schema if it doesn't exist
   - Create the volume if it doesn't exist
   - Save data to `/Volumes/{CATALOG}/{SCHEMA}/raw_data/`

## Local Files vs Databricks Catalog

### Local Project Files (Your Computer)
- **Location**: Can be anywhere on your computer (e.g., `/Users/raven.mukherjee/claude-code/`)
- **Contents**: `scripts/`, `docs/`, `config.py` - these are just local files
- **Purpose**: Code, documentation, configuration
- **Not Required**: These files don't need to be in any `ai-dev-kit` directory

### Databricks Catalog (Cloud)
- **Location**: In your Databricks workspace (e.g., `hls_amer_catalog`)
- **Contents**: Tables, volumes, schemas - your actual data
- **Purpose**: Storage and processing of healthcare data
- **Connection**: Accessed via Databricks MCP server (configured in `.mcp.json`)

**Key Point**: Your local `docs/` and `scripts/` folders are completely separate from Databricks. They can be anywhere on your computer. The `ai-dev-kit` is just a tool that provides connectivity - it doesn't require any specific directory structure.

## AI Dev Kit Usage

The AI dev kit provides:
- **MCP Server**: Databricks connectivity (configured in `.mcp.json`)
- **Skills**: Best practices and patterns (in `.claude/skills/`)
- **Not Required**: You don't need to use the `ai_dev_kit` catalog or any specific directory structure

Your local project files (`scripts/`, `docs/`, `config.py`) can be anywhere - they're just regular files on your computer. The catalog `hls_amer_catalog` exists in Databricks, not locally.
