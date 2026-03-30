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

**DataHub** is a high-performance, distributed version control system (VCS) engineered specifically for high-volume binary datasets and machine learning models. Unlike traditional source control systems (e.g., Git) which struggle with 10GB+ binary files, DataHub utilizes a **Content-Addressable Storage (CAS)** architecture backed by a PostgreSQL **Merkle Directed Acyclic Graph (DAG)**.

The system provide atomic versioning, O(1) deduplication, and automated metadata extraction via both a **React-based Glassmorphism Dashboard** and a **Native Python CLI**.

---

## 🏗️ 2. Detailed Module Architecture

### **Module 1: DAG Architecture & Schema Design**

The foundational versioning layer built on PostgreSQL.

* **Merkle Commit Chain**: Every commit hash is a deterministic function of `(blob_hash + parent_hash + timestamp + message)`, making history cryptographically tamper-evident.
* **Recursive CTE Traversal**: Implemented PostgreSQL recursive CTEs for full ancestry queries in a single SQL round-trip.
* **Tree Snapshot Model**: Each commit stores a `tree_hash` representing the complete repository state, enabling instant rollback to any historical point.
* **Atomic Transactions**: Push operations (blob write + metadata index + commit record) are wrapped in a single DB transaction.

### **Module 2: Storage Engine & CAS Deduplication**

The binary data layer — zero-redundancy by design.

* **SHA-256 Content Addressing**: Every file's identity is its content hash; renaming a file costs zero additional storage.
* **O(1) Deduplication**: Before any write, the hash is looked up; if found, only a new DB pointer is created.
* **Storage Quotas**: Per-user disk quotas (Default: 100MB) enforced before any write operation.
* **Garbage Collection**: Admin-triggered GC identifies and removes blobs no longer referenced by any commit tree.

### **Module 3: Client-Side CLI**

A production-ready terminal interface with full platform parity.

* **Full Session Management**: `login`, `whoami`, `logout` with local JWT caching.
* **Statistical Diffing**: `diff <commit_a> <commit_b>` provides column-level statistical comparisons.
* **Machine-Readable Output**: Global `--json` flag for CI/CD and automation script integration.

### **Module 4: High-Performance Networking**

Hardened against memory exhaustion at any file size.

* **64 KB Chunked Streaming**: `shutil.copyfileobj()` processes uploads/downloads sequentially; RAM usage remains O(1) even for 50GB files.
* **Background Tasks**: Metadata extraction and LLaMA-3 summarization are dispatched asynchronously; server returns `202 Accepted` immediately.
* **Streaming Downloads**: File pulls are streamed directly to disk on the client without full buffering.

### **Module 5: Metadata Extraction & AI Summarization**

Automatically triggered on every push.

* **Deep Statistical Profiling**: mean, median, mode, SD, skewness, kurtosis, and null percentages via Pandas/SciPy.
* **Data Quality Detection**: Automated detection of duplicate row counts and schema cardinality.
* **AI Summarization**: Generates a 3-sentence natural language report via **LLaMA-3** to highlight data quality issues.
* **Interactive AI Agent**: Chat with your data directly from the terminal or dashboard.

### **Module 6: Query Language & Reporting**

Domain-specific filtering engine built on PostgreSQL JSONB binary indexing.

* **Metric Filtering**: `log --metric 'accuracy > 0.95'` returns all matching commits.
* **Compound Queries**: Supports AND/OR combinations across multiple metrics (e.g., `accuracy > 0.9, loss < 0.1`).
* **Time-Range Filtering**: Restrict commit history to specific date windows using ISO-8601 strings.

---

## 💻 3. Comprehensive CLI Reference

The DataHub CLI is a production-ready interface for interacting with the platform.

### Authentication & Session Management
| Command | Description | Example |
| :--- | :--- | :--- |
| `login` | Authenticate & save JWT locally | `python datahub_cli.py login <user> <pass>` |
| `whoami` | Display active user and role | `python datahub_cli.py whoami` |
| `logout` | Securely clear session token | `python datahub_cli.py logout` |

### Statistics & Version Control
| Command | Description | Example |
| :--- | :--- | :--- |
| `push` | Stream upload dataset & index | `python datahub_cli.py push "data.csv" "proj" -m "update"` |
| `pull` | Download specific version by ID | `python datahub_cli.py pull <id> -o "./data.csv"` |
| `log` | View history with metric filters | `python datahub_cli.py log "proj" --metric "acc > 0.9"` |
| `diff` | Statistical comparison of commits | `python datahub_cli.py diff <hash1> <hash2>` |

### Branching & Collaboration
| Command | Description | Example |
| :--- | :--- | :--- |
| `branch list` | List all branches in a project | `python datahub_cli.py branch list "proj"` |
| `branch create`| Initialize a new branch pointer | `python datahub_cli.py branch create "proj" "dev"` |
| `branch delete`| Remove a branch pointer | `python datahub_cli.py branch delete "proj" "dev"` |

### Pull Requests (PR) Workflow
| Command | Description | Example |
| :--- | :--- | :--- |
| `pr list` | View open merge proposals | `python datahub_cli.py pr list "proj"` |
| `pr create` | Propose merge from src to target | `python datahub_cli.py pr create "proj" "Title" "dev" "main"` |
| `pr merge` | Execute final Merkle DAG merge | `python datahub_cli.py pr merge <pr_id>` |

### AI Assistant & Scripting
| Command | Description | Example |
| :--- | :--- | :--- |
| `chat` | LLaMA-3 conversation about data | `python datahub_cli.py chat "Why is accuracy low?" --id <id>` |
| `--json` | Global flag for JSON output | `python datahub_cli.py log "proj" --json` |

---

## 🛠️ 4. Technical Execution & Deployment

### Database Initialization

1. Ensure PostgreSQL is running on `5432`.
2. Create the database: `CREATE DATABASE datahub_db;`
3. Run schema setup: `psql -d datahub_db -f backend/db_setup.sql`

### Backend Setup (FastAPI)

```bash
cd backend
python -m venv venv
# Windows: .\venv\Scripts\activate | Mac: source venv/bin/activate
pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Frontend Setup (React/Vite)

```bash
cd frontend
npm install
npm run dev
```

---

## 🔒 5. Security (Role-Based Access Control)

DataHub implements strict RBAC at the database and API level:

* 👑 **Admin**: Full access to all projects, audit logs, and Garbage Collection.
* 📊 **Analyst**: Can upload data, update metrics, and query metadata.
* 👁 **Viewer**: Read-only access to logs and project history.

---

## 🎯 6. Project Evaluation Notice

**Key Deliverables Included:**

- Atomic Merkle-DAG Versioning.
- Automated Metadata extraction (Module 5).
- Integrated LLaMA-3 Virtual Assistant.
- High-performance binary streaming (Module 4).

<div align="center">
  <b>D A T A H U B</b> — <i>Built for Data-Driven Engineering</i>
</div>
