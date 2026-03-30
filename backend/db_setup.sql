-- =============================================================================
-- DataHub Production Setup Script
-- Module Coverage: 1-6 (DAG, CAS, CLI, Networking, AI, Query Engine)
-- =============================================================================

-- =============================================================================
-- SECTION 1: ROLE & USER MANAGEMENT (IDEMPOTENT & ROBUST)
-- =============================================================================

DO $$
BEGIN
    -- Handle datahub_admin
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'datahub_admin') THEN
        CREATE ROLE datahub_admin WITH LOGIN PASSWORD 'datahub_pass' CREATEDB CREATEROLE;
    ELSE
        ALTER ROLE datahub_admin WITH LOGIN PASSWORD 'datahub_pass' CREATEDB CREATEROLE;
    END IF;

    -- Handle datahub_analyst
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'datahub_analyst') THEN
        CREATE ROLE datahub_analyst WITH LOGIN PASSWORD 'analyst_pass';
    ELSE
        ALTER ROLE datahub_analyst WITH LOGIN PASSWORD 'analyst_pass';
    END IF;

    -- Handle datahub_viewer
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'datahub_viewer') THEN
        CREATE ROLE datahub_viewer WITH LOGIN PASSWORD 'viewer_pass';
    ELSE
        ALTER ROLE datahub_viewer WITH LOGIN PASSWORD 'viewer_pass';
    END IF;

    -- Clean slate for permissions
    EXECUTE 'REVOKE ALL ON ALL TABLES IN SCHEMA public FROM datahub_admin, datahub_analyst, datahub_viewer';
    EXECUTE 'REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM datahub_admin, datahub_analyst, datahub_viewer';
    EXECUTE 'REVOKE ALL ON SCHEMA public FROM datahub_admin, datahub_analyst, datahub_viewer';
END
$$;


-- =============================================================================
-- SECTION 2: SCHEMA & TABLES (CLEAN SLATE & MERKLE DAG)
-- =============================================================================

-- CRITICAL: Clear legacy tables to resolve schema mismatches (sha256_hash vs blob_hash)
DROP TABLE IF EXISTS pull_requests CASCADE;
DROP TABLE IF EXISTS branches CASCADE;
DROP TABLE IF EXISTS audit_log CASCADE;
DROP TABLE IF EXISTS metadata CASCADE;
DROP TABLE IF EXISTS commits CASCADE;
DROP TABLE IF EXISTS tree_entry CASCADE;
DROP TABLE IF EXISTS tree CASCADE;
DROP TABLE IF EXISTS blobs CASCADE;
DROP TABLE IF EXISTS projects CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- 2.1 Users table (for authentication)
CREATE TABLE users (
    id               SERIAL PRIMARY KEY,
    username         VARCHAR(100) UNIQUE NOT NULL,
    email            VARCHAR(255) UNIQUE NOT NULL,
    hashed_password  TEXT NOT NULL,
    role             VARCHAR(50) DEFAULT 'viewer' NOT NULL,  -- admin, analyst, viewer
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- 2.2 Projects / Repositories table
CREATE TABLE projects (
    id               SERIAL PRIMARY KEY,
    name             VARCHAR(200) UNIQUE NOT NULL,
    description      TEXT,
    owner_id         INTEGER REFERENCES users(id) ON DELETE CASCADE,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- 2.3 Blobs: Content-Addressable Storage registry (Module 2)
CREATE TABLE blobs (
    blob_hash        CHAR(64) PRIMARY KEY,
    size_bytes       BIGINT NOT NULL,
    storage_path     TEXT NOT NULL,
    is_compressed    BOOLEAN DEFAULT FALSE,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- 2.4 Trees: Directory snapshot anchors
CREATE TABLE tree (
    tree_hash        CHAR(64) PRIMARY KEY
);

-- 2.5 Tree Entries: Manifest mapping directory contents
CREATE TABLE tree_entry (
    id               SERIAL PRIMARY KEY,
    tree_hash        CHAR(64) REFERENCES tree(tree_hash) ON DELETE CASCADE,
    name             VARCHAR(255) NOT NULL,
    mode             VARCHAR(50) NOT NULL,   -- 'file' or 'dir'
    object_hash      CHAR(64) NOT NULL       -- FK -> blob_hash or tree_hash
);

-- 2.6 Commits: Cryptographic ancestry (Module 1 - Merkle DAG)
CREATE TABLE commits (
    commit_hash      CHAR(64) PRIMARY KEY,
    parent_hash      CHAR(64) REFERENCES commits(commit_hash),
    project_id       INTEGER REFERENCES projects(id) ON DELETE CASCADE,
    author_id        INTEGER REFERENCES users(id),
    message          TEXT NOT NULL,
    tree_hash        CHAR(64) REFERENCES tree(tree_hash),
    branch           VARCHAR(100) DEFAULT 'main',
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- 2.7 Metadata: The Intelligence Layer (Module 5)
CREATE TABLE metadata (
    id               SERIAL PRIMARY KEY,
    target_hash      CHAR(64) NOT NULL,      -- FK -> COMMIT or BLOB hash
    stats            JSONB,                  -- row_count, metrics, AI summary
    indexed_at       TIMESTAMPTZ DEFAULT NOW()
);

-- 2.8 Audit log (Module 6)
CREATE TABLE audit_log (
    id               SERIAL PRIMARY KEY,
    user_id          INTEGER REFERENCES users(id),
    action           VARCHAR(100) NOT NULL,
    table_name       VARCHAR(100),
    record_id        TEXT,
    details          JSONB,
    performed_at     TIMESTAMPTZ DEFAULT NOW()
);

-- 2.9 Branches tracker
CREATE TABLE branches (
    id               SERIAL PRIMARY KEY,
    project_id       INTEGER REFERENCES projects(id) ON DELETE CASCADE,
    name             VARCHAR(100) NOT NULL,
    head_commit_hash CHAR(64) REFERENCES commits(commit_hash),
    UNIQUE(project_id, name)
);

-- 2.10 Pull Requests workflow
CREATE TABLE pull_requests (
    id               SERIAL PRIMARY KEY,
    project_id       INTEGER REFERENCES projects(id) ON DELETE CASCADE,
    title            VARCHAR(255) NOT NULL,
    description      TEXT,
    source_branch    VARCHAR(100) NOT NULL,
    target_branch    VARCHAR(100) NOT NULL,
    status           VARCHAR(50) DEFAULT 'open', -- open, merged, closed
    author_id        INTEGER REFERENCES users(id),
    created_at       TIMESTAMPTZ DEFAULT NOW()
);


-- =============================================================================
-- SECTION 3: GRANT PERMISSIONS (FULL RBAC COVERAGE)
-- =============================================================================

-- Role 1: Admin
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO datahub_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO datahub_admin;
GRANT USAGE, CREATE ON SCHEMA public TO datahub_admin;

-- Role 2: Analyst (Full CRUD on Data, but no DBA rights)
GRANT SELECT, UPDATE ON projects, commits, blobs, tree, tree_entry, metadata, branches, pull_requests TO datahub_analyst;
GRANT SELECT ON users, audit_log TO datahub_analyst;
GRANT USAGE ON SCHEMA public TO datahub_analyst;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO datahub_analyst;

-- Role 3: Viewer (Read-only)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub_viewer;
GRANT USAGE ON SCHEMA public TO datahub_viewer;


-- =============================================================================
-- SECTION 4: SEED DATA & VIEWS
-- =============================================================================

-- Default Users (Passwords: Admin@123)
INSERT INTO users (username, email, hashed_password, role) VALUES
  ('admin_user',   'admin@datahub.io',   '$2b$12$FDzsrJTFHrjCpIOxis2tjT108J.OhK9mpLxIR5dSPln88Ysj', 'admin')
ON CONFLICT (username) DO NOTHING;

-- Default Project
INSERT INTO projects (name, description, owner_id) VALUES
  ('ml-models-v2', 'Integrated high-performance data lineage repository', 1)
ON CONFLICT (name) DO NOTHING;

-- Global Metadata View (Module 6 Reporting)
CREATE OR REPLACE VIEW v_commit_metadata AS
SELECT
    c.commit_hash,
    c.message,
    c.branch,
    c.created_at AS committed_at,
    p.name AS project,
    m.stats->>'row_count' AS row_count,
    m.stats->>'ai_summary' AS ai_summary
FROM commits c
JOIN projects p ON c.project_id = p.id
LEFT JOIN metadata m ON m.target_hash = c.commit_hash;

GRANT SELECT ON v_commit_metadata TO datahub_analyst, datahub_viewer;
