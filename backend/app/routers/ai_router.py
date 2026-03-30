"""AI Agent router: interactive chat + metadata summarizer."""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional

from app.db.database import get_db
from app.models.models import Metadata, User
from app.routers.auth import get_current_user
from app.ai.ai_agent import chat_with_ai

router = APIRouter(prefix="/ai", tags=["AI Agent"])


class ChatRequest(BaseModel):
    question: str
    metadata_id: Optional[int] = None


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
    context = ""
    if req.metadata_id:
        record = db.query(Metadata).filter(Metadata.id == req.metadata_id).first()
        if record:
            import json
            st = record.stats or {}
            context = f"""Dataset: {st.get("file_name")} ({str(st.get("file_type")).upper()})
Rows: {st.get("row_count")}, Columns: {st.get("column_count")}
Schema: {json.dumps(st.get("columns_schema"), indent=2) if st.get("columns_schema") else 'N/A'}
Statistics: {json.dumps(st.get("statistics"), indent=2) if st.get("statistics") else 'N/A'}
Custom Metrics: {json.dumps(st.get("custom_metrics")) if st.get("custom_metrics") else 'None'}
AI Summary: {st.get("ai_summary") or 'N/A'}"""

    answer = await chat_with_ai(req.question, context)
    return {"answer": answer, "metadata_id": req.metadata_id}
