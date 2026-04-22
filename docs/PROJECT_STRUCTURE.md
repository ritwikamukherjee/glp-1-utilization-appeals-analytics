# Project Structure Explained

## Overview

This project has two separate parts:

1. **Local Files** (on your computer) - Code, docs, config
2. **Databricks Catalog** (in the cloud) - Your data storage

## Local Project Structure

```
/Users/raven.mukherjee/claude-code/
├── config.py                          # Configuration (catalog, schema, etc.)
├── README.md                          # Project overview
└── appeals-review/                    # Main project folder
    ├── scripts/
    │   └── generate_healthcare_data.py # Data generation script
    └── docs/
        ├── DATA_MODEL.md              # Data model documentation
        ├── SETUP.md                   # Setup guide
        └── PROJECT_STRUCTURE.md        # This file
```

**These files can be anywhere on your computer** - they don't need to be in any `ai-dev-kit` directory. They're just regular files/folders.

## Databricks Catalog Structure

```
hls_amer_catalog (your catalog)
└── appeals_review (schema - will be created)
    └── raw_data (volume - will be created)
        ├── members/                  # Parquet files
        ├── providers/                 # Parquet files
        ├── eligibility/               # Parquet files
        ├── prior_authorizations/      # Parquet files
        ├── claims/                    # Parquet files
        └── appeals/                   # Parquet files
```

**This exists in Databricks**, not on your local computer.

## How They Connect

1. **Local script** (`appeals-review/scripts/generate_healthcare_data.py`) reads **local config** (`config.py`)
2. Script connects to **Databricks** via MCP server (configured in `.mcp.json`)
3. Script creates schema/volume in **Databricks catalog** (`hls_amer_catalog`)
4. Script generates data and saves to **Databricks volume** (`/Volumes/hls_amer_catalog/appeals_review/raw_data/`)

## AI Dev Kit

The AI dev kit is installed at `~/.ai-dev-kit/` and provides:
- MCP server for Databricks connectivity
- Skills and best practices

**You don't need to put your project files there.** Your project files (`claude-code/`) are completely separate and can be anywhere.

## Summary

| Item | Location | Purpose |
|------|----------|---------|
| `appeals-review/scripts/`, `appeals-review/docs/`, `config.py` | Your computer (anywhere) | Code and documentation |
| `hls_amer_catalog` | Databricks workspace | Data storage |
| AI dev kit | `~/.ai-dev-kit/` | Connectivity tool (separate) |

**Bottom line**: Your project files are local and can be anywhere. The catalog is in Databricks. They're connected via the MCP server, but they're separate things.
