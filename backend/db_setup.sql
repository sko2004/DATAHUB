-- =============================================================================
-- DataHub Module 5: Metadata Extraction & Indexing
-- PostgreSQL Setup Script with Role-Based Access Control (DBA)
-- =============================================================================

-- 1. Create the database (run as postgres superuser)
-- CREATE DATABASE datahub_db;
-- \c datahub_db

-- =============================================================================
-- SECTION 1: ROLE & USER MANAGEMENT (DBA)
-- =============================================================================

-- Drop existing roles/users if re-running
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'datahub_admin') THEN DROP ROLE datahub_admin; END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'datahub_analyst') THEN DROP ROLE datahub_analyst; END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'datahub_viewer') THEN DROP ROLE datahub_viewer; END IF;
END
$$;

-- Role 1: Admin — full DBA rights (READ, WRITE, CREATE TABLE, CREATE USER)
CREATE ROLE datahub_admin WITH LOGIN PASSWORD 'datahub_pass' CREATEDB CREATEROLE;

-- Role 2: Analyst — VIEW + UPDATE rights, NO create user/table
CREATE ROLE datahub_analyst WITH LOGIN PASSWORD 'analyst_pass';

-- Role 3: Viewer — READ-ONLY rights
CREATE ROLE datahub_viewer WITH LOGIN PASSWORD 'viewer_pass';


-- =============================================================================
-- SECTION 2: SCHEMA & TABLES
-- =============================================================================

-- Users table (for authentication)
CREATE TABLE IF NOT EXISTS users (
    id               SERIAL PRIMARY KEY,
    username         VARCHAR(100) UNIQUE NOT NULL,
    email            VARCHAR(255) UNIQUE NOT NULL,
    hashed_password  TEXT NOT NULL,
    role             VARCHAR(50) DEFAULT 'viewer' NOT NULL,  -- admin, analyst, viewer
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Projects / Repositories table
CREATE TABLE IF NOT EXISTS projects (
    id               SERIAL PRIMARY KEY,
    name             VARCHAR(200) UNIQUE NOT NULL,
    description      TEXT,
    owner_id         INTEGER REFERENCES users(id) ON DELETE CASCADE,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Blobs: Content-Addressable Storage registry
CREATE TABLE IF NOT EXISTS blobs (
    sha256_hash      CHAR(64) PRIMARY KEY,
    file_size_bytes  BIGINT NOT NULL,
    mime_type        VARCHAR(100),
    storage_path     TEXT NOT NULL,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Commits: Recursive parent-child relationship (Merkle DAG)
CREATE TABLE IF NOT EXISTS commits (
    commit_hash      CHAR(64) PRIMARY KEY,
    parent_hash      CHAR(64) REFERENCES commits(commit_hash),
    project_id       INTEGER REFERENCES projects(id) ON DELETE CASCADE,
    author_id        INTEGER REFERENCES users(id),
    message          TEXT NOT NULL,
    tree_json        JSONB,                  -- { "path": "sha256_hash", ... }
    branch           VARCHAR(100) DEFAULT 'main',
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- *** MODULE 5 CORE TABLE ***
-- Metadata: Extracted statistics indexed on commit
CREATE TABLE IF NOT EXISTS metadata (
    id               SERIAL PRIMARY KEY,
    commit_hash      CHAR(64) REFERENCES commits(commit_hash) ON DELETE CASCADE,
    blob_hash        CHAR(64) REFERENCES blobs(sha256_hash),
    file_name        VARCHAR(500) NOT NULL,
    file_type        VARCHAR(20) NOT NULL,   -- csv, json, parquet
    row_count        INTEGER,
    column_count     INTEGER,
    columns_schema   JSONB,                  -- { "col_name": "dtype", ... }
    statistics       JSONB,                  -- { "col": { mean, std, min, max, nulls } }
    distributions    JSONB,                  -- { "col": { "histogram": [...] } }
    custom_metrics   JSONB,                  -- model accuracy, F1, etc.
    ai_summary       TEXT,                   -- AI-generated natural language summary
    indexed_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Audit log for all write operations
CREATE TABLE IF NOT EXISTS audit_log (
    id               SERIAL PRIMARY KEY,
    user_id          INTEGER REFERENCES users(id),
    action           VARCHAR(100) NOT NULL,
    table_name       VARCHAR(100),
    record_id        TEXT,
    details          JSONB,
    performed_at     TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- SECTION 3: GRANT PERMISSIONS (Role-Based Access Control)
-- =============================================================================

-- Grant full privileges to admin on all tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO datahub_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO datahub_admin;
GRANT USAGE, CREATE ON SCHEMA public TO datahub_admin;

-- Analyst: SELECT + UPDATE on data tables (no CREATE TABLE, no CREATE USER)
GRANT SELECT, UPDATE ON projects, commits, blobs, metadata TO datahub_analyst;
GRANT SELECT ON users, audit_log TO datahub_analyst;
GRANT USAGE ON SCHEMA public TO datahub_analyst;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO datahub_analyst;

-- Viewer: SELECT ONLY on all tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub_viewer;
GRANT USAGE ON SCHEMA public TO datahub_viewer;

-- =============================================================================
-- SECTION 4: SEED DATA
-- =============================================================================

-- Insert demo users (passwords are bcrypt hashed; plaintext below for demo)
-- admin_user: password = "Admin@123"
-- analyst_user: password = "Analyst@123"
-- viewer_user: password = "Viewer@123"
INSERT INTO users (username, email, hashed_password, role) VALUES
  ('admin_user',   'admin@datahub.io',   '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW', 'admin'),
  ('analyst_user', 'analyst@datahub.io', '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW', 'analyst'),
  ('viewer_user',  'viewer@datahub.io',  '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW', 'viewer')
ON CONFLICT (username) DO NOTHING;

-- Insert a demo project
INSERT INTO projects (name, description, owner_id) VALUES
  ('ml-models-v2', 'Machine learning model weights and training datasets', 1)
ON CONFLICT (name) DO NOTHING;

-- =============================================================================
-- SECTION 5: VIEWS (for Module 6 query engine support)
-- =============================================================================

CREATE OR REPLACE VIEW v_commit_metadata AS
SELECT
    c.commit_hash,
    c.message,
    c.branch,
    c.created_at AS committed_at,
    u.username AS author,
    p.name AS project,
    m.file_name,
    m.file_type,
    m.row_count,
    m.column_count,
    m.custom_metrics,
    m.ai_summary
FROM commits c
JOIN users u ON c.author_id = u.id
JOIN projects p ON c.project_id = p.id
LEFT JOIN metadata m ON m.commit_hash = c.commit_hash;

-- Grant view access
GRANT SELECT ON v_commit_metadata TO datahub_analyst, datahub_viewer;
