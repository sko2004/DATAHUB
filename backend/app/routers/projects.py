"""Projects and commits router."""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.models.models import Project, Commit, User
from app.routers.auth import get_current_user

router = APIRouter(prefix="/projects", tags=["Projects & Commits"])


@router.get("/")
def list_projects(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    projects = db.query(Project).all()
    return [
        {
            "id": p.id,
            "name": p.name,
            "description": p.description,
            "owner": p.owner.username if p.owner else None,
            "created_at": p.created_at.isoformat() if p.created_at else None,
        }
        for p in projects
    ]


@router.get("/{project_name}/log")
def project_log(
    project_name: str,
    branch: Optional[str] = None,
    metric_filter: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Module 6 integration: datahub log --metric "accuracy > 0.9"
    Traverses the commit DAG for a project with optional metric filter.
    """
    project = db.query(Project).filter(Project.name == project_name).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    query = db.query(Commit).filter(Commit.project_id == project.id)
    if branch:
        query = query.filter(Commit.branch == branch)
    commits = query.order_by(Commit.created_at.desc()).all()

    result = []
    for c in commits:
        entry = {
            "commit_hash": c.commit_hash,
            "parent_hash": c.parent_hash,
            "message": c.message,
            "branch": c.branch,
            "author": c.author.username if c.author else None,
            "created_at": c.created_at.isoformat() if c.created_at else None,
            "metadata": [],
        }
        for m in c.metadata_entries:
            metrics = m.custom_metrics or {}
            # Apply optional metric filter (e.g. "accuracy > 0.9")
            if metric_filter:
                try:
                    key, op, val = metric_filter.split()
                    val = float(val)
                    metric_val = metrics.get(key, {})
                    if isinstance(metric_val, dict):
                        metric_val = metric_val.get("latest", 0)
                    if op == ">" and not (metric_val > val):
                        continue
                    if op == "<" and not (metric_val < val):
                        continue
                    if op == ">=" and not (metric_val >= val):
                        continue
                    if op == "<=" and not (metric_val <= val):
                        continue
                    if op == "==" and not (metric_val == val):
                        continue
                except Exception:
                    pass
            entry["metadata"].append({
                "file_name": m.file_name,
                "row_count": m.row_count,
                "column_count": m.column_count,
                "custom_metrics": metrics,
                "ai_summary": m.ai_summary,
            })
        result.append(entry)

    return result
