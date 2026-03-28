from sqlalchemy import (
    Column, Integer, String, Text, BigInteger,
    DateTime, ForeignKey, JSON, CHAR
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.db.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=False)
    hashed_password = Column(Text, nullable=False)
    role = Column(String(50), default="viewer", nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    projects = relationship("Project", back_populates="owner")
    commits = relationship("Commit", back_populates="author")


class Project(Base):
    __tablename__ = "projects"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(200), unique=True, nullable=False, index=True)
    description = Column(Text)
    owner_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    owner = relationship("User", back_populates="projects")
    commits = relationship("Commit", back_populates="project")


class Blob(Base):
    __tablename__ = "blobs"

    sha256_hash = Column(CHAR(64), primary_key=True)
    file_size_bytes = Column(BigInteger, nullable=False)
    mime_type = Column(String(100))
    storage_path = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    metadata_entries = relationship("Metadata", back_populates="blob")


class Commit(Base):
    __tablename__ = "commits"

    commit_hash = Column(CHAR(64), primary_key=True)
    parent_hash = Column(CHAR(64), ForeignKey("commits.commit_hash"), nullable=True)
    project_id = Column(Integer, ForeignKey("projects.id", ondelete="CASCADE"))
    author_id = Column(Integer, ForeignKey("users.id"))
    message = Column(Text, nullable=False)
    tree_json = Column(JSONB)
    branch = Column(String(100), default="main")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    project = relationship("Project", back_populates="commits")
    author = relationship("User", back_populates="commits")
    parent = relationship("Commit", remote_side=[commit_hash])
    metadata_entries = relationship("Metadata", back_populates="commit")


class Metadata(Base):
    """MODULE 5 CORE TABLE: Extracted file statistics indexed per commit."""
    __tablename__ = "metadata"

    id = Column(Integer, primary_key=True, index=True)
    commit_hash = Column(CHAR(64), ForeignKey("commits.commit_hash", ondelete="CASCADE"))
    blob_hash = Column(CHAR(64), ForeignKey("blobs.sha256_hash"))
    file_name = Column(String(500), nullable=False)
    file_type = Column(String(20), nullable=False)   # csv, json, parquet
    row_count = Column(Integer)
    column_count = Column(Integer)
    columns_schema = Column(JSONB)    # { col_name: dtype }
    statistics = Column(JSONB)        # { col: { mean, std, min, max, nulls } }
    distributions = Column(JSONB)     # { col: { histogram: [...] } }
    custom_metrics = Column(JSONB)    # model accuracy, F1, etc.
    ai_summary = Column(Text)         # AI-generated natural language summary
    indexed_at = Column(DateTime(timezone=True), server_default=func.now())

    commit = relationship("Commit", back_populates="metadata_entries")
    blob = relationship("Blob", back_populates="metadata_entries")


class AuditLog(Base):
    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    action = Column(String(100), nullable=False)
    table_name = Column(String(100))
    record_id = Column(Text)
    details = Column(JSONB)
    performed_at = Column(DateTime(timezone=True), server_default=func.now())
