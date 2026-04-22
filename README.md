<div align="center">
  <h1>🚀 DataHub</h1>
  <p><b>A Content-Addressable Version Control System for Large-Scale Data Lineage</b></p>

  [![Python FastAPI](https://img.shields.io/badge/Backend-FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
  [![React Vite](https://img.shields.io/badge/Frontend-React_Vite-61DAFB?style=for-the-badge&logo=react&logoColor=black)](https://reactjs.org/)
  [![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
  [![Groq LLaMA3](https://img.shields.io/badge/AI-Groq_LLaMA_3-f55036?style=for-the-badge)](https://groq.com/)

</div>

<br/>

## 📖 Overview

**DataHub** is a high-performance, distributed version control system (VCS) engineered specifically for high-volume binary datasets and machine learning models. Built to overcome the limitations of traditional source control systems (like Git) when handling 10GB+ binary files, DataHub utilizes a **Content-Addressable Storage (CAS)** architecture backed by a PostgreSQL-powered **Merkle Directed Acyclic Graph (DAG)**.

## ✨ Key Features

### 🤖 AI-Powered Data Intelligence
*   **Conversational Analytics**: Integrated Groq-powered assistant (LLaMA-3) to analyze datasets using natural language.
*   **Automated Insights**: Generates executive summaries and statistical reports on every commit.
*   **Context-Aware Chat**: Ask specific questions about your data (e.g., "Explain the distribution of column revenue") with instant markdown-rendered responses.

### 📊 Automated Metadata Explorer
*   **Deep Profiling**: Automatic extraction of descriptive statistics (mean, median, skewness, cardinality) for CSV, JSON, and Parquet files.
*   **Interactive Dashboard**: A modern web interface to explore project history, lineage, and statistical trends.
*   **Statistical Diffing**: Compare two versions of a dataset to see how distributions and metrics have shifted over time.

### 🛡️ Enterprise-Grade Engine
*   **Zero-Redundancy Storage**: SHA-256 CAS deduplication ensures that identical file content is only stored once across the entire system.
*   **Immutable Lineage**: Every change is tracked in a Merkle DAG, providing a cryptographically verifiable audit trail.
*   **Memory-Efficient Streaming**: Sequential 64KB chunked processing allows handling of massive datasets without RAM exhaustion.

---

## 🏗️ Architecture

### **Versioning & Merkle DAG**
The core lineage is managed via an immutable commit chain. Each commit hash is a deterministic function of the data content, parent reference, and metadata, ensuring full reproducibility.

### **Storage & Deduplication**
By addressing data by its hash (content-addressable), DataHub achieves O(1) deduplication. Renaming or moving files within the system incurs no additional storage overhead.

### **Query Engine**
A high-performance query layer enables filtering of massive commit logs based on custom metrics (e.g., `accuracy > 0.95`) stored in optimized JSONB fields.

---

## 💻 CLI Reference

DataHub provides a robust command-line interface for managing data as code. 

### **Basic Setup & Auth**
| Command | Description | Example |
| :--- | :--- | :--- |
| `login` | Authenticate and save session token | `python datahub_cli.py login admin_user Admin@123` |
| `whoami` | Show current active session info | `python datahub_cli.py whoami` |
| `logout` | Securely clear local session | `python datahub_cli.py logout` |
| `init` | Initialize a local DataHub mapping | `python datahub_cli.py init` |

### **Project & Version Control**
| Command | Description | Example |
| :--- | :--- | :--- |
| `projects`| List all accessible project repos | `python datahub_cli.py projects` |
| `push` | Stream upload & index a dataset | `python datahub_cli.py push "C:\Users\saura\Desktop\DATA\data_nifty\1_ADANIENT.csv" nifty -m "Initial commit"` |
| `log` | View commit history for a project | `python datahub_cli.py log nifty` |
| `pull` | Download a specific version | `python datahub_cli.py pull <id> -o "./local_copy.csv"` |

### **Collaboration & Intelligence**
| Command | Description | Example |
| :--- | :--- | :--- |
| `branch` | Manage project branches | `python datahub_cli.py branch --create dev-branch` |
| `pr` | Manage Pull Requests | `python datahub_cli.py pr --create` |
| `diff` | Compare stats between two commits | `python datahub_cli.py diff <hash1> <hash2>` |
| `chat` | Ask AI about a specific dataset | `python datahub_cli.py chat "Show me column trends" --id <id>` |

---

### 📝 Detailed Pushing Example

To push the Adani Nifty dataset to a project named `nifty`:

```bash
# 1. Login to your account
python datahub_cli.py login admin_user Admin@123

# 2. Push the dataset
python datahub_cli.py push "C:\Users\saura\Desktop\DATA\data_nifty\1_ADANIENT.csv" nifty -m "Initial ADANIENT dataset"

# 3. Verify in the commit log
python datahub_cli.py log nifty
```

---

## 🛠️ Installation & Setup

### 1. Database Configuration
DataHub requires PostgreSQL. Initialize the schema using the provided SQL script:
```bash
psql -d datahub_db -f backend/db_setup.sql
```

### 2. Backend Service (FastAPI)
```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### 3. Frontend Dashboard (React/Vite)
```bash
cd frontend
npm install
npm run dev
```

---

## ⚡ Quick Start (Demo Credentials)

For local development and testing, the following pre-seeded accounts can be used to explore the platform:

| Role | Username | Password | Access Level |
| :--- | :--- | :--- | :--- |
| **Admin** | `admin_user` | `Admin@123` | Full Access + DBA Audit Logs |
| **Analyst** | `analyst_user` | `Analyst@123` | Push/Pull + AI Metadata Chat |
| **Viewer** | `viewer_user` | `Viewer@123` | Read-only access to Logs |

---

## 🔐 Access Control

DataHub implements strict Role-Based Access Control (RBAC). Below are the default system roles:

| Role | Capabilities |
| :--- | :--- |
| **Admin** | Full system access, DBA audit logs, and user management. |
| **Analyst** | Push/pull datasets, branch management, and AI chat access. |
| **Viewer** | Read-only access to commit logs and metadata summaries. |

---

<div align="center">
  <b>D A T A H U B</b> — <i>Scale your data lineage with confidence.</i>
</div>
