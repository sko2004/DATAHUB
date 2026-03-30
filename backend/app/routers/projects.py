"""Projects and commits router."""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.models.models import Project, Commit, User, Metadata
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
    since: Optional[str] = None,
    until: Optional[str] = None,
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
    
    # Module 6: Time-range filtering
    from datetime import datetime
    if since:
        try: query = query.filter(Commit.created_at >= datetime.fromisoformat(since))
        except: pass
    if until:
        try: query = query.filter(Commit.created_at <= datetime.fromisoformat(until))
        except: pass

        # Module 1: Fetch commit history for the project
    from sqlalchemy import text
    sql = text("""
        SELECT commit_hash, parent_hash, message, branch, author_id, created_at, project_id
        FROM commits
        WHERE project_id = :project_id
        AND (:branch IS NULL OR branch = :branch)
        ORDER BY created_at DESC;
    """)
    
    commits = db.execute(sql, {"project_id": project.id, "branch": branch}).fetchall()

    result = []
    for c in commits:
        # c is a raw Row â€” look up the author username from the users table
        from app.models.models import User as UserModel
        author_obj = db.query(UserModel).filter(UserModel.id == c.author_id).first()
        entry = {
            "commit_hash": c.commit_hash,
            "parent_hash": c.parent_hash,
            "message": c.message,
            "branch": c.branch,
            "author": author_obj.username if author_obj else None,
            "created_at": c.created_at.isoformat() if c.created_at else None,
            "metadata": [],
        }
        c_metadata = db.query(Metadata).filter(Metadata.target_hash == c.commit_hash).all()
        for m in c_metadata:
            st = m.stats or {}
            metrics = st.get("custom_metrics") or {}
            # Module 6: Compound metric filter (e.g. "accuracy > 0.9, loss < 0.1")
            if metric_filter:
                conditions = [c.strip() for c in metric_filter.split(",")]
                all_match = True
                for cond in conditions:
                    try:
                        import re
                        match = re.search(r'([a-zA-Z_0-9]+)\s*(>|<|>=|<=|==)\s*([0-9.]+)', cond)
                        if not match: continue
                        key, op, val = match.groups()
                        val = float(val)
                        metric_val = metrics.get(key, {})
                        if isinstance(metric_val, dict):
                            metric_val = metric_val.get("latest", 0)
                        
                        m_val = float(metric_val)
                        if op == ">" and not (m_val > val): all_match = False
                        elif op == "<" and not (m_val < val): all_match = False
                        elif op == ">=" and not (m_val >= val): all_match = False
                        elif op == "<=" and not (m_val <= val): all_match = False
                        elif op == "==" and not (m_val == val): all_match = False
                        if not all_match: break
                    except: all_match = False; break
                if not all_match: continue
            entry["metadata"].append({
                "file_name": st.get("file_name"),
                "row_count": st.get("row_count"),
                "column_count": st.get("column_count"),
                "custom_metrics": metrics,
                "ai_summary": st.get("ai_summary"),
            })
        result.append(entry)

    return result


@router.get("/compare")
def compare_commits(
    commit_a: str,
    commit_b: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Module 3: diff <commit_a> <commit_b> â€” column-level statistical diff."""
    meta_a = db.query(Metadata).filter(Metadata.target_hash == commit_a).first()
    meta_b = db.query(Metadata).filter(Metadata.target_hash == commit_b).first()
    
    if not meta_a or not meta_b:
        raise HTTPException(status_code=404, detail="One or both commits not found")
        
    stats_a = meta_a.stats.get("statistics", {})
    stats_b = meta_b.stats.get("statistics", {})
    
    diff = {}
    all_cols = set(stats_a.keys()) | set(stats_b.keys())
    
    for col in all_cols:
        s_a = stats_a.get(col, {})
        s_b = stats_b.get(col, {})
        
        diff[col] = {
            "dtype_changed": s_a.get("dtype") != s_b.get("dtype"),
            "row_count_diff": (meta_b.stats.get("row_count") or 0) - (meta_a.stats.get("row_count") or 0),
            "metrics": {}
        }
        
        for k in ["mean", "max", "min", "null_pct", "unique_count"]:
            if k in s_a and k in s_b:
                try: diff[col]["metrics"][k] = round(float(s_b[k]) - float(s_a[k]), 4)
                except: pass
                
    return {
        "commit_a": commit_a,
        "commit_b": commit_b,
        "column_diffs": diff
    }

