"""
Module 5 Core Router: File Upload, Metadata Extraction & Indexing
Automatically extracts and stores metadata on every commit/upload.
"""
import hashlib
import os
import shutil
from pathlib import Path
from typing import List, Optional

from fastapi import (
    APIRouter, Depends, HTTPException, UploadFile, File, Form, status, BackgroundTasks
)
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.models.models import Blob, Commit, Metadata, Project, AuditLog, User
from app.routers.auth import get_current_user, require_role
from app.services.metadata_extractor import analyze_file, compute_sha256
from app.ai.ai_agent import generate_ai_summary
from app.config import get_settings

settings = get_settings()
router = APIRouter(prefix="/metadata", tags=["Module 5 - Metadata Extraction"])

ALLOWED_EXTENSIONS = {".csv", ".json", ".parquet"}


def _ensure_upload_dir():
    Path(settings.upload_dir).mkdir(parents=True, exist_ok=True)


# ---- Schemas ----
class MetadataOut(BaseModel):
    id: int
    commit_hash: str
    blob_hash: Optional[str]
    file_name: str
    file_type: str
    row_count: Optional[int]
    column_count: Optional[int]
    columns_schema: Optional[dict]
    statistics: Optional[dict]
    distributions: Optional[dict]
    custom_metrics: Optional[dict]
    ai_summary: Optional[str]
    indexed_at: str

    class Config:
        from_attributes = True


class CommitRequest(BaseModel):
    project_name: str
    message: str
    branch: str = "main"
    custom_metrics: Optional[dict] = None  # manually passed metrics (e.g., model accuracy)


# ---- Endpoints ----

@router.post("/upload-and-commit", status_code=201)
async def upload_and_commit(
    background_tasks: BackgroundTasks,
    project_name: str = Form(...),
    message: str = Form(...),
    branch: str = Form("main"),
    custom_metrics: Optional[str] = Form(None),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Core Module 5 endpoint:
    1. Accept file upload (CSV/JSON/Parquet)
    2. Compute SHA-256 hash (CAS deduplication)
    3. Save blob if new
    4. Create commit record
    5. Trigger metadata extraction
    6. Generate AI summary
    7. Index everything in PostgreSQL
    """
    # Validate file type
    suffix = Path(file.filename).suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{suffix}'. Allowed: {ALLOWED_EXTENSIONS}"
        )

    _ensure_upload_dir()

    # Step 1: Save temp file
    temp_path = os.path.join(settings.upload_dir, f"_tmp_{file.filename}")
    with open(temp_path, "wb") as f_out:
        shutil.copyfileobj(file.file, f_out)

    # Step 2: Compute hash (CAS)
    blob_hash = compute_sha256(temp_path)
    file_size = os.path.getsize(temp_path)
    final_path = os.path.join(settings.upload_dir, blob_hash)

    # Step 3: Deduplication — skip blob write if already exists
    existing_blob = db.query(Blob).filter(Blob.sha256_hash == blob_hash).first()
    if not existing_blob:
        shutil.move(temp_path, final_path)
        blob = Blob(
            sha256_hash=blob_hash,
            file_size_bytes=file_size,
            mime_type=file.content_type,
            storage_path=final_path,
        )
        db.add(blob)
        db.flush()
        is_duplicate = False
    else:
        os.remove(temp_path)
        final_path = existing_blob.storage_path
        is_duplicate = True

    # Step 4: Get or create project
    project = db.query(Project).filter(Project.name == project_name).first()
    if not project:
        project = Project(name=project_name, owner_id=current_user.id)
        db.add(project)
        db.flush()

    # Get last commit for parent hash
    last_commit = (
        db.query(Commit)
        .filter(Commit.project_id == project.id, Commit.branch == branch)
        .order_by(Commit.created_at.desc())
        .first()
    )
    parent_hash = last_commit.commit_hash if last_commit else None

    # Build commit hash (SHA256 of parent+message+blob)
    commit_input = f"{parent_hash or ''}{message}{blob_hash}".encode()
    commit_hash = hashlib.sha256(commit_input).hexdigest()

    # Step 5: Create commit
    tree_json = {file.filename: blob_hash}
    commit = Commit(
        commit_hash=commit_hash,
        parent_hash=parent_hash,
        project_id=project.id,
        author_id=current_user.id,
        message=message,
        tree_json=tree_json,
        branch=branch,
    )
    db.add(commit)
    db.flush()

    # Step 6: Extract metadata (Module 5 core)
    try:
        meta_dict = analyze_file(final_path, file_type=suffix.lstrip("."))
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=422, detail=f"Metadata extraction failed: {str(e)}")

    # Merge custom metrics if provided
    external_metrics = {}
    if custom_metrics:
        import json
        try:
            external_metrics = json.loads(custom_metrics)
        except Exception:
            pass
    meta_dict["custom_metrics"] = {**meta_dict.get("custom_metrics", {}), **external_metrics}

    # Step 7: Generate AI summary
    ai_summary = await generate_ai_summary(meta_dict)

    # Step 8: Store metadata record
    meta_record = Metadata(
        commit_hash=commit_hash,
        blob_hash=blob_hash,
        file_name=file.filename,
        file_type=suffix.lstrip("."),
        row_count=meta_dict.get("row_count"),
        column_count=meta_dict.get("column_count"),
        columns_schema=meta_dict.get("columns_schema"),
        statistics=meta_dict.get("statistics"),
        distributions=meta_dict.get("distributions"),
        custom_metrics=meta_dict.get("custom_metrics"),
        ai_summary=ai_summary,
    )
    db.add(meta_record)

    # Audit log
    log = AuditLog(
        user_id=current_user.id,
        action="COMMIT",
        table_name="commits",
        record_id=commit_hash,
        details={
            "file": file.filename,
            "project": project_name,
            "is_duplicate": is_duplicate,
            "rows": meta_dict.get("row_count"),
        },
    )
    db.add(log)
    db.commit()

    return {
        "status": "success",
        "commit_hash": commit_hash,
        "is_duplicate_blob": is_duplicate,
        "project": project_name,
        "branch": branch,
        "metadata": {
            "file_name": file.filename,
            "file_type": suffix.lstrip("."),
            "row_count": meta_dict.get("row_count"),
            "column_count": meta_dict.get("column_count"),
            "columns": list(meta_dict.get("columns_schema", {}).keys()),
            "custom_metrics": meta_dict.get("custom_metrics"),
            "ai_summary": ai_summary,
        },
    }


@router.get("/", response_model=List[dict])
def list_metadata(
    project_name: Optional[str] = None,
    file_type: Optional[str] = None,
    limit: int = 50,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List all indexed metadata records (filterable)."""
    query = db.query(Metadata)
    if project_name:
        query = query.join(Commit).join(Project).filter(Project.name == project_name)
    if file_type:
        query = query.filter(Metadata.file_type == file_type)

    records = query.order_by(Metadata.indexed_at.desc()).limit(limit).all()
    return [
        {
            "id": r.id,
            "commit_hash": r.commit_hash,
            "file_name": r.file_name,
            "file_type": r.file_type,
            "row_count": r.row_count,
            "column_count": r.column_count,
            "columns_schema": r.columns_schema,
            "custom_metrics": r.custom_metrics,
            "ai_summary": r.ai_summary,
            "indexed_at": r.indexed_at.isoformat() if r.indexed_at else None,
        }
        for r in records
    ]


@router.get("/{metadata_id}", response_model=dict)
def get_metadata_detail(
    metadata_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get full metadata including statistics and distributions."""
    record = db.query(Metadata).filter(Metadata.id == metadata_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Metadata record not found")
    return {
        "id": record.id,
        "commit_hash": record.commit_hash,
        "blob_hash": record.blob_hash,
        "file_name": record.file_name,
        "file_type": record.file_type,
        "row_count": record.row_count,
        "column_count": record.column_count,
        "columns_schema": record.columns_schema,
        "statistics": record.statistics,
        "distributions": record.distributions,
        "custom_metrics": record.custom_metrics,
        "ai_summary": record.ai_summary,
        "indexed_at": record.indexed_at.isoformat() if record.indexed_at else None,
    }


@router.put("/{metadata_id}/metrics", dependencies=[Depends(require_role("admin", "analyst"))])
def update_metrics(
    metadata_id: int,
    metrics: dict,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Analyst/Admin: Update custom metrics on an existing metadata record."""
    record = db.query(Metadata).filter(Metadata.id == metadata_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Metadata record not found")

    existing = record.custom_metrics or {}
    existing.update(metrics)
    record.custom_metrics = existing

    log = AuditLog(
        user_id=current_user.id,
        action="UPDATE_METRICS",
        table_name="metadata",
        record_id=str(metadata_id),
        details=metrics,
    )
    db.add(log)
    db.commit()
    db.refresh(record)
    return {"message": "Metrics updated", "custom_metrics": record.custom_metrics}


@router.get("/stats/summary", response_model=dict)
def get_global_stats(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Dashboard summary statistics."""
    total_commits = db.query(Commit).count()
    total_blobs = db.query(Blob).count()
    total_metadata = db.query(Metadata).count()
    total_projects = db.query(Project).count()

    from sqlalchemy import func
    total_rows = db.query(func.sum(Metadata.row_count)).scalar() or 0
    total_storage = db.query(func.sum(Blob.file_size_bytes)).scalar() or 0

    return {
        "total_commits": total_commits,
        "total_blobs": total_blobs,
        "total_indexed_files": total_metadata,
        "total_projects": total_projects,
        "total_rows_indexed": total_rows,
        "total_storage_bytes": total_storage,
    }
