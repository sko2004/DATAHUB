<div align="center">
  <h1>🚀 DataHub</h1>
  <p><b>A Content-Addressable Version Control System for Large-Scale Data Lineage</b></p>

  [![Python FastAPI](https://img.shields.io/badge/Backend-FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
  [![React Vite](https://img.shields.io/badge/Frontend-React_Vite-61DAFB?style=for-the-badge&logo=react&logoColor=black)](https://reactjs.org/)
  [![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
  [![Groq LLaMA3](https://img.shields.io/badge/AI-Groq_LLaMA_3-f55036?style=for-the-badge)](https://groq.com/)
</div>

<br/>

## 📖 1. Executive Summary
**DataHub** is a distributed version control system (VCS) engineered specifically for high-volume binary datasets and machine learning models. Unlike traditional source control systems (e.g., Git) which degrade in performance with large binary files, DataHub utilizes a **Content-Addressable Storage (CAS)** architecture backed by a PostgreSQL relational database. The system provides atomic versioning, automatic data deduplication, and granular lineage tracking for AI/ML workflows via both a **React-based Graphical Interface** and a **Native Python Command Line Interface (CLI)**.

---

## 🛑 2. Problem Statement
In modern machine learning pipelines, the *"Reproducibility Crisis"* is a significant bottleneck. While logic code is well-versioned via Git, the associated datasets (gigabytes in size) and model weights are often managed manually. This natively leads to:

* 🚨 **Redundancy:** Massive storage waste due to saving multiple copies of slightly modified datasets.
* 🔗 **Loss of Lineage:** Inability to mathematically prove which dataset version produced a specific model array.
* ⚡ **Concurrency Issues:** Race conditions when multiple researchers attempt to modify data registries simultaneously.

---

## 🧠 3. System Architecture & Database Logic
The core innovation of DataHub lies in its implementation of a **Merkle Directed Acyclic Graph (DAG)** within a PostgreSQL environment to ensure data integrity and storage efficiency at an enterprise scale.

### The Content-Addressable Storage (CAS) Model
DataHub actively separates the logical state of a project from its physical storage:
* 📉 **O(1) Deduplication:** When a file is committed, the API natively computes its `SHA-256` hash. If the exact hash already exists in the blobs registry, the physical upload stream is skipped entirely, and only a new pointer reference is created.
* 🔒 **Immutable Blob Storage:** Binary objects are stored via our FastAPI high-performance streaming protocol and indexed strictly by their hash. Data can never be silently corrupted.

### The Database Schema (DBMS Core)
<details>
<summary><b>Click to expand architecture details</b></summary>

* **`commits` Table:** Implements a recursive relationship (`parent_hash` FK) to mathematically build the entire version history tree backward.
* **`blobs` Table:** High-fidelity tracking of file sizes, formats, and absolute storage locations bridging to the `commits` tree.
* **`metadata` Table:** A highly queryable index containing rich `JSONB` structures representing statistical properties (row counts, column datatypes, distributions) seamlessly extracted upon upload.

</details>

---

## ⚙️ 4. Functional Modules (Technical Scope Mapping)

The backend systems of DataHub are explicitly divided into 6 distinct engineering modules. All aspects have been successfully mapped to the active architecture:

| Module | Core Responsibility | Physical Implementation |
|:---:|---|---|
| **1** | **DAG Architecture & Schema Design**: Implementation of recursive SQL schemas (CTEs). | Located in `backend/db_setup.sql`. |
| **2** | **Storage Engine & Deduplication Algorithms**: Optimized "Put/Get" system hashing SHA-256 strings. | Located in `metadata_extractor.py`. |
| **3** | **Client-Side CLI**: Python-facing terminal configurations (`datahub init, push, pull`). | Handled by `datahub_cli.py`. |
| **4** | **High-Performance Networking**: Sequential, chunk-based binary streams for 10GB+ file protections. | Parsed via `shutil.copyfileobj` in router endpoints. |
| **5** | **Metadata Extraction & Indexing**: Automated parsing logic indexing CSV/JSON/Parquet distributions. | Active inside `metadata_extractor.py` Pandas engine. |
| **6** | **Query Language & Reporting Engine**: Dynamic domain-specific SQL generating command queries over dataset filters. | Accessible via `datahub_cli.py log --metric`. |

---

## 🛠️ 5. Technical Stack

> **Database Engine:** PostgreSQL *(Recursive CTE modeling and native `JSONB`)*  
> **Backend Processing:** Python / FastAPI *(High-concurrency streaming handles)*  
> **AI Orchestration Framework:** Groq LLaMA-3 *(Contextualizing semantic data via LLM)*  
> **Graphical Frontend:** React / Vite *(Glassmorphism SPA)*  
> **CLI Client Ecosystem:** Python / Argparse  

---

## 💻 6. Execution & Deployment Guide

This workspace is cleanly configured into two exact sub-components (`/frontend/` and `/backend/`).

### Step 1: Initialize Database & Roles
Launch your Postgres engine initialized on port `5432`.
```sql
CREATE DATABASE datahub_db;
\c datahub_db
\i backend/db_setup.sql
```

### Step 2: Start the Backend (API Server)
Ensure `backend/.env` is configured stringing to your DB.
```bash
cd backend
python -m venv venv
# Windows: .\venv\Scripts\activate
# Mac: source venv/bin/activate
pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Step 3: Launch the React Dashboard
Open a new terminal tab:
```bash
cd frontend
npm install
npm run dev
```

### Step 4: Using the Python CLI Interface
Interact manually with DataHub bypassing the React implementation entirely using the custom Module 3 script:

```bash
# 1. Authenticate session keys natively
python datahub_cli.py login analyst_user Analyst@123

# 2. Send files via multi-part data streams (Triggers Mod 2, 4 & 5)
python datahub_cli.py push "dataset.csv" "cli-project" -m "Initial upload" --branch "main"

# 3. Request dynamic DAG metrics mapping (Triggers Mod 1 & 6)
python datahub_cli.py log "cli-project" --metric "accuracy > 0.90"
```

---

## 🎯 7. Conclusion
DataHub successfully demonstrates an incredibly advanced proficiency in Database Management Systems. By actively applying complex mathematical data structures natively within a relational model, the ecosystem solves a critical real-world infrastructure problem prioritizing absolute data integrity, massive storage optimization, and complete system scalability. 

<div align="center">
  <b>D A T A H U B</b>
</div>