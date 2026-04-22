# DataHub: Advanced Project Description Report

## 🚀 Executive Summary
**DataHub** is an enterprise-grade, distributed version control system (VCS) engineered specifically for the lifecycle management of high-volume binary datasets and machine learning models. By combining **Content-Addressable Storage (CAS)** with **AI-driven metadata intelligence**, DataHub provides cryptographically verifiable lineage, automated data profiling, and natural language analytical capabilities.

---

## 🤖 AI-Powered Data Intelligence (Module 5)

The AI layer in DataHub is designed to transform raw statistics into actionable business insights using state-of-the-art Large Language Models.

### **1. Core Engine**
- **Infrastructure**: Powered by **Groq LLaMA-3.1-8B**, utilizing ultra-fast inference to provide near-instant responses.
- **Integration Layer**: The `ai_agent.py` service handles prompt engineering, session management, and fallback logic.

### **2. Technical Implementation**
- **Automated Summarization**: Every commit triggers an asynchronous task that builds a JSON-encoded summary of extracted statistics. The LLM then generates a 3-sentence executive report:
    1. **Scope**: Scale and domain analysis.
    2. **Numeric Evidence**: 2-3 specific numeric facts (means, ranges).
    3. **Quality/Alerts**: Identification of skew, null peaks, or anomalies.
- **Context-Aware Chat**: The `chat_with_ai` endpoint implements strict system prompting to prevent hallucination. It uses aggregate metadata (means, top-frequency items) as a "Source of Truth," explicitly informing the model that it cannot see raw row-level data to ensure privacy and accuracy.
- **Robust Fallback**: If the API key is missing or the service is down, a `_rule_based_summary` engine takes over, extracting key insights (peak values, high-null warnings) using deterministic algorithms.

---

## 📊 Automated Metadata Profiling & Storage

DataHub treats metadata as a first-class citizen, automatically indexing every byte of data uploaded to the system.

### **1. Deep Extraction Workflow**
- **Trigger**: Uploading a file via the CLI or GUI initiates a `BackgroundTasks` process in FastAPI.
- **Extraction Engine**: 
    - **Numeric Columns**: Calculates mean, median, standard deviation, skewness, kurtosis, and quartiles.
    - **Categorical Columns**: Detects cardinality and top-5 frequency distributions.
    - **Data Quality**: Tracks null percentages and duplicate row counts.
- **Custom Metric Auto-Detection**: A smart scanner identifies keywords like `accuracy`, `loss`, `f1`, and `auc` to automatically extract machine learning performance metrics and store them in a persistent JSONB field in PostgreSQL.

### **2. Storage & Lineage (CAS Architecture)**
- **SHA-256 Deduplication**: Files are indexed by their content hash. If multiple users upload the same 10GB dataset, it is stored physically only once.
- **Merkle DAG**: Lineage is managed via an immutable commit chain where each `Commit` points to a `Tree` of `TreeEntry` (blobs), ensuring full reproducibility.
- **Storage Quotas**: Integrated logic prevents users from exceeding storage limits (default 100MB per user for demo).

---

## 💻 GUI: Central Intelligence Dashboard

The frontend is a high-performance **React/Vite** application designed for speed and clarity.

### **1. Navigation & State Management**
- **Framework**: Built with React Hooks for real-time UI updates and state persistence.
- **Iconography**: Uses `lucide-react` for a premium, developer-focused aesthetic.
- **Modules**:
    - **Overview**: Real-time counters for projects, commits, deduplicated storage, and indexed rows.
    - **Explore Metadata**: A list of indexed files with snippets of AI-generated summaries.
    - **Dataset Detail**: A deep-dive view including:
        - Interactive histograms of distributions.
        - Full schema definitions.
        - **Raw Data Explorer**: A safe-sampled view of the top 100 rows fetched directly from the CAS blob storage.
    - **Project Log**: A visual commit history showing branches, authors, and data deltas.

### **2. Integrated AI Assistant**
- A dedicated "AI Data Agent" tab allows users to query their entire project catalog using natural language, providing a conversational interface for complex data retrieval.

---

## 🛡️ Enterprise Security & Governance

### **1. Role-Based Access Control (RBAC)**
- **Admin**: Full system management and access to the **DBA Audit Console**.
- **Analyst**: Capability to push/pull data and interact with the AI suite.
- **Viewer**: Read-only access to commit logs and metadata summaries.

### **2. DBA Audit System**
- Every critical action (login, commit, pull, chat query) is recorded in the `audit_logs` table.
- Admins can monitor system usage, identify performance bottlenecks, and audit data access patterns through a dedicated UI.

---

## 📂 Project Organization
```text
DATAHUB/
├── backend/app/
│   ├── ai/             # Groq LLM integration & prompt logic
│   ├── routers/        # API endpoints (Metadata, AI, Auth, Projects)
│   ├── services/       # Core profiling engine (Pandas/Scipy)
│   └── models/         # SQLAlchemy DB schemas
├── frontend/src/
│   ├── App.jsx         # Unified UI logic & component architecture
│   └── index.css       # Premium Design System (tokens & styles)
└── datahub_cli.py      # Powerful CLI for data-as-code workflows
```

---

<div align="center">
  <b>D A T A H U B</b> — <i>Powering the next generation of data-centric AI.</i>
</div>
