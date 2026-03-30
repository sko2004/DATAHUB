"""
Module 5 Core Router: File Upload, Metadata Extraction & Indexing
Automatically extracts and stores metadata on every commit/upload.
"""
import hashlib
import os
import shutil
from pathlib import Path
from typing import List, Optional
import numpy as np

from fastapi import (
    APIRouter, Depends, HTTPException, UploadFile, File, Form, status, BackgroundTasks
)
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.models.models import Blob, Commit, Metadata, Project, AuditLog, User, Tree, TreeEntry
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



# ---- Tasks for Background Processing (Mod 4 & 5) ----
async def process_metadata_task(final_path: str, suffix: str, commit_hash: str, custom_metrics_str: str, file_name: str):
    """Heavy lift of metadata extraction & AI summary in background."""
    from app.db.database import SessionLocal
    db = SessionLocal()
    try:
        from app.services.metadata_extractor import analyze_file
        from app.ai.ai_agent import generate_ai_summary
        
        # 1. Extract
        meta_dict = analyze_file(final_path, file_type=suffix.lstrip("."))
        
        # 2. Merge custom metrics
        external_metrics = {}
        if custom_metrics_str:
            import json
            try:
                external_metrics = json.loads(custom_metrics_str)
            except: pass
        meta_dict["custom_metrics"] = {**meta_dict.get("custom_metrics", {}), **external_metrics}
        
        # 3. AI Summary
        ai_summary = await generate_ai_summary(meta_dict)
        
        # 4. Save metadata record
        meta_record = Metadata(
            target_hash=commit_hash,
            stats={
                "file_name": file_name,
                "file_type": suffix.lstrip("."),
                "row_count": meta_dict.get("row_count"),
                "column_count": meta_dict.get("column_count"),
                "columns_schema": meta_dict.get("columns_schema"),
                "statistics": meta_dict.get("statistics"),
                "distributions": meta_dict.get("distributions"),
                "custom_metrics": meta_dict.get("custom_metrics"),
                "ai_summary": ai_summary,
            }
        )
        db.add(meta_record)
        db.commit()
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Background Metadata Process Failed: {e}")
    finally:
        db.close()

@router.post("/upload-and-commit", status_code=202)
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

    # Step 0: Storage Quota check (Module 2) - 100MB limit per user
    from sqlalchemy import func
    user_storage = db.query(func.sum(Blob.size_bytes))\
                     .join(TreeEntry, TreeEntry.object_hash == Blob.blob_hash)\
                     .join(Tree, Tree.tree_hash == TreeEntry.tree_hash)\
                     .join(Commit, Commit.tree_hash == Tree.tree_hash)\
                     .filter(Commit.author_id == current_user.id).scalar() or 0
    
    if user_storage > 100 * 1024 * 1024:
        raise HTTPException(
            status_code=403, 
            detail=f"Storage quota exceeded. Currently using {user_storage / (1024*1024):.1f} MB"
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
    existing_blob = db.query(Blob).filter(Blob.blob_hash == blob_hash).first()
    if not existing_blob:
        shutil.move(temp_path, final_path)
        blob = Blob(
            blob_hash=blob_hash,
            size_bytes=file_size,
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

    # Step 5: Create or get Tree and create Commit
    import json
    tree_dict = {file.filename: blob_hash}
    tree_hash = hashlib.sha256(json.dumps(tree_dict, sort_keys=True).encode()).hexdigest()

    existing_tree = db.query(Tree).filter(Tree.tree_hash == tree_hash).first()
    if not existing_tree:
        new_tree = Tree(tree_hash=tree_hash)
        db.add(new_tree)
        db.flush()
        
        tree_entry = TreeEntry(
            tree_hash=tree_hash,
            name=file.filename,
            mode='file',
            object_hash=blob_hash
        )
        db.add(tree_entry)
        db.flush()

    commit = Commit(
        commit_hash=commit_hash,
        parent_hash=parent_hash,
        project_id=project.id,
        author_id=current_user.id,
        message=message,
        tree_hash=tree_hash,
        branch=branch,
    )
    db.add(commit)
    db.flush()

    # Step 6: Dispatch async processing (Module 4 performance optimization)
    background_tasks.add_task(
        process_metadata_task,
        final_path,
        suffix,
        commit_hash,
        custom_metrics,
        file.filename
    )

    # Audit log
    log = AuditLog(
        user_id=current_user.id,
        action="COMMIT_ACCEPTED",
        table_name="commits",
        record_id=commit_hash,
        details={
            "file": file.filename,
            "project": project_name,
            "is_duplicate": is_duplicate,
            "indexing": "background"
        },
    )
    db.add(log)
    db.commit()

    return {
        "status": "accepted",
        "message": "Upload successful. Metadata indexing is running in the background.",
        "commit_hash": commit_hash,
        "is_duplicate_blob": is_duplicate,
        "project": project_name,
        "branch": branch
    }

@router.delete("/gc", dependencies=[Depends(require_role("admin"))])
def garbage_collect_blobs(db: Session = Depends(get_db)):
    """Module 2: Garbage collection for orphaned blobs."""
    # Find blobs not referenced by any tree_entry
    referenced_hashes = db.query(TreeEntry.object_hash).distinct().all()
    referenced_hashes = [r[0] for r in referenced_hashes]
    
    orphans = db.query(Blob).filter(Blob.blob_hash.notin_(referenced_hashes)).all()
    count = 0
    for blob in orphans:
        if os.path.exists(blob.storage_path):
            os.remove(blob.storage_path)
        db.delete(blob)
        count += 1
    db.commit()
    return {"status": "success", "removed_blobs": count}


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
        query = query.join(Commit, Metadata.target_hash == Commit.commit_hash)\
                     .join(Project).filter(Project.name == project_name)
    
    if file_type:
        # Module 6: Use PostgreSQL JSONB operator to filter stats->>'file_type'
        from sqlalchemy import text
        query = query.filter(Metadata.stats.op("->>")("file_type") == file_type)

    records = query.order_by(Metadata.indexed_at.desc()).limit(limit).all()
    
    result = []
    for r in records:
        st = r.stats or {}
        # Try to find commit to get project name
        project_name = "unknown"
        commit = db.query(Commit).filter(Commit.commit_hash == r.target_hash).first()
        if commit and commit.project:
            project_name = commit.project.name

        result.append({
            "id": r.id,
            "target_hash": r.target_hash,
            "project_name": project_name,
            "file_name": st.get("file_name"),
            "file_type": st.get("file_type"),
            "row_count": st.get("row_count"),
            "column_count": st.get("column_count"),
            "columns_schema": st.get("columns_schema"),
            "custom_metrics": st.get("custom_metrics"),
            "ai_summary": st.get("ai_summary"),
            "indexed_at": r.indexed_at.isoformat() if r.indexed_at else None,
        })
    return result


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

    from sqlalchemy import func, Integer
    total_rows = db.query(func.sum(Metadata.stats['row_count'].astext.cast(Integer))).scalar() or 0
    total_storage = db.query(func.sum(Blob.size_bytes)).scalar() or 0

    return {
        "total_commits": total_commits,
        "total_blobs": total_blobs,
        "total_indexed_files": total_metadata,
        "total_projects": total_projects,
        "total_rows_indexed": total_rows,
        "total_storage_bytes": total_storage,
    }


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
    st = record.stats or {}
    
    # Trace project name
    project_name = "unknown"
    commit = db.query(Commit).filter(Commit.commit_hash == record.target_hash).first()
    if commit and commit.project:
        project_name = commit.project.name
    
    # Trace base blob hash
    blob_hash = record.target_hash
    if commit and commit.tree and commit.tree.entries:
        # For tree commits, the "data" is the first blob entry
        blob_hash = commit.tree.entries[0].object_hash

    return {
        "id": record.id,
        "target_hash": record.target_hash,
        "blob_hash": blob_hash,
        "project_name": project_name,
        "file_name": st.get("file_name"),
        "file_type": st.get("file_type"),
        "row_count": st.get("row_count"),
        "column_count": st.get("column_count"),
        "columns_schema": st.get("columns_schema"),
        "statistics": st.get("statistics"),
        "distributions": st.get("distributions"),
        "custom_metrics": st.get("custom_metrics"),
        "ai_summary": st.get("ai_summary"),
        "indexed_at": record.indexed_at.isoformat() if record.indexed_at else None,
    }


@router.get("/{metadata_id}/data")
def get_dataset_sample(
    metadata_id: int,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Retrieve the actual rows of a dataset from its blob storage."""
    record = db.query(Metadata).filter(Metadata.id == metadata_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Metadata record not found")

    blob = None
    commit = db.query(Commit).filter(Commit.commit_hash == record.target_hash).first()
    if commit and commit.tree and commit.tree.entries:
        blob = db.query(Blob).filter(Blob.blob_hash == commit.tree.entries[0].object_hash).first()
    else:
        blob = db.query(Blob).filter(Blob.blob_hash == record.target_hash).first()

    if not blob:
        raise HTTPException(status_code=404, detail="Metadata or associated file not found")

    from app.services.metadata_extractor import load_file_to_dataframe
    try:
        df = load_file_to_dataframe(blob.storage_path, record.stats.get("file_type", "csv"))
        # Handle nan values for JSON serialization
        df = df.head(limit).replace({np.nan: None})
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load data: {str(e)}")


@router.get("/{metadata_id}/download")
def download_dataset(
    metadata_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Module 4 & 6: Download original file via streaming response."""
    record = db.query(Metadata).filter(Metadata.id == metadata_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Metadata record not found")

    blob = None
    commit = db.query(Commit).filter(Commit.commit_hash == record.target_hash).first()
    if commit and commit.tree and commit.tree.entries:
        blob = db.query(Blob).filter(Blob.blob_hash == commit.tree.entries[0].object_hash).first()
    else:
        # Fallback to direct hash lookup if linked directly to blob
        blob = db.query(Blob).filter(Blob.blob_hash == record.target_hash).first()

    if not blob or not os.path.exists(blob.storage_path):
        raise HTTPException(status_code=404, detail="Physical blob file not found in storage.")

    file_name = record.stats.get("file_name", f"data_{blob.blob_hash[:8]}")
    return FileResponse(
        path=blob.storage_path,
        media_type='application/octet-stream',
        filename=file_name
    )
