"""AI Agent router: interactive chat + metadata summarizer."""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional

from app.db.database import get_db
from app.models.models import Metadata, User, Commit
from app.routers.auth import get_current_user
from app.ai.ai_agent import chat_with_ai

router = APIRouter(prefix="/ai", tags=["AI Agent"])


class ChatRequest(BaseModel):
    question: str
    project_id: Optional[int] = None


@router.post("/chat")
async def ai_chat(
    req: ChatRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Integrated AI Agent endpoint.
    Users can ask questions about their data. If metadata_id is provided,
    the AI gets the full context of that dataset's statistics.
    """
    context = "No specific data context available. Provide general assistance regarding DataHub features."
    if req.project_id:
        from app.models.models import Project, Commit, Metadata
        import json

        project = db.query(Project).filter(Project.id == req.project_id).first()
        repo_name = project.name if project else "Unknown"

        # 1. Fetch Commit History (last 20 commits)
        commits = db.query(Commit).filter(Commit.project_id == req.project_id).order_by(Commit.created_at.desc()).limit(20).all()
        commit_ctx = []
        for c in commits:
            commit_ctx.append(f"- Commit {c.commit_hash[:8]}: {c.message} (by {c.author.username if c.author else 'Unknown'} at {c.created_at})")
        
        commit_history_str = "\n".join(commit_ctx) if commit_ctx else "No commits found for this project."

        # 2. Fetch Metadata / Dataset stats
        records = db.query(Metadata).join(Commit, Metadata.target_hash == Commit.commit_hash).filter(Commit.project_id == req.project_id).all()
        ctx_list = []
        if records:
            for record in records:
                st = record.stats or {}
                ctx_item = f"""Dataset File: {st.get("file_name")} ({str(st.get("file_type")).upper()})
Rows: {st.get("row_count")}, Columns: {st.get("column_count")}
Schema: {json.dumps(st.get("columns_schema"), indent=2) if st.get("columns_schema") else 'N/A'}
Statistics: {json.dumps(st.get("statistics"), indent=2) if st.get("statistics") else 'N/A'}
Custom Metrics: {json.dumps(st.get("custom_metrics")) if st.get("custom_metrics") else 'None'}
AI Summary: {st.get("ai_summary") or 'N/A'}"""
                ctx_list.append(ctx_item)
        
        dataset_ctx_str = "\n\n---\n\n".join(ctx_list) if ctx_list else "No datasets indexed in this project."

        context = f"""REPOSITORY / PROJECT: {repo_name}

COMMIT HISTORY (Most Recent First):
{commit_history_str}

INDEXED DATASETS & METADATA:
{dataset_ctx_str}
"""

    answer = await chat_with_ai(req.question, context)
    return {"answer": answer, "project_id": req.project_id}
