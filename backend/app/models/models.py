from sqlalchemy import (
    Column, Integer, String, Text, BigInteger,
    DateTime, ForeignKey, JSON, CHAR, Boolean
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

    blob_hash = Column(CHAR(64), primary_key=True)
    size_bytes = Column(BigInteger, nullable=False)
    storage_path = Column(Text, nullable=False)
    is_compressed = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Tree(Base):
    __tablename__ = "tree"

    tree_hash = Column(CHAR(64), primary_key=True)

    entries = relationship("TreeEntry", back_populates="tree", cascade="all, delete-orphan")
    commits = relationship("Commit", back_populates="tree")


class TreeEntry(Base):
    __tablename__ = "tree_entry"

    id = Column(Integer, primary_key=True, index=True)
    tree_hash = Column(CHAR(64), ForeignKey("tree.tree_hash", ondelete="CASCADE"), nullable=False)
    name = Column(String(255), nullable=False)
    mode = Column(String(50), nullable=False)
    object_hash = Column(CHAR(64), nullable=False)

    tree = relationship("Tree", back_populates="entries")


class Commit(Base):
    __tablename__ = "commits"

    commit_hash = Column(CHAR(64), primary_key=True)
    parent_hash = Column(CHAR(64), ForeignKey("commits.commit_hash"), nullable=True)
    project_id = Column(Integer, ForeignKey("projects.id", ondelete="CASCADE"))
    author_id = Column(Integer, ForeignKey("users.id"))
    message = Column(Text, nullable=False)
    tree_hash = Column(CHAR(64), ForeignKey("tree.tree_hash"))
    branch = Column(String(100), default="main")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    project = relationship("Project", back_populates="commits")
    author = relationship("User", back_populates="commits")
    parent = relationship("Commit", remote_side=[commit_hash])
    tree = relationship("Tree", back_populates="commits")


class Metadata(Base):
    """MODULE 5 CORE TABLE: Queryable Data Intelligence Layer."""
    __tablename__ = "metadata"

    id = Column(Integer, primary_key=True, index=True)
    target_hash = Column(CHAR(64), nullable=False, index=True)  # FK -> Commit or Blob hash
    stats = Column(JSONB)                           # schema, row_count, metrics, accuracy, etc.
    indexed_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    # Link metadata back to the commit that produced it
    commit = relationship("Commit", primaryjoin="Metadata.target_hash == Commit.commit_hash", foreign_keys=[target_hash], uselist=False, viewonly=True)


class AuditLog(Base):
    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    action = Column(String(100), nullable=False)
    table_name = Column(String(100))
    record_id = Column(Text)
    details = Column(JSONB)
    performed_at = Column(DateTime(timezone=True), server_default=func.now())


class Branch(Base):
    __tablename__ = "branches"

    id = Column(Integer, primary_key=True, index=True)
    project_id = Column(Integer, ForeignKey("projects.id", ondelete="CASCADE"))
    name = Column(String(100), nullable=False)
    head_commit_hash = Column(CHAR(64), ForeignKey("commits.commit_hash"))

    project = relationship("Project")


class PullRequest(Base):
    __tablename__ = "pull_requests"

    id = Column(Integer, primary_key=True, index=True)
    project_id = Column(Integer, ForeignKey("projects.id", ondelete="CASCADE"))
    title = Column(String(255), nullable=False)
    description = Column(Text)
    source_branch = Column(String(100), nullable=False)
    target_branch = Column(String(100), nullable=False)
    status = Column(String(50), default="open")
    author_id = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    project = relationship("Project")
    author = relationship("User")
