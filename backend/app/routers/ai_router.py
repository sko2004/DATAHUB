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
            context = f"""Dataset: {record.file_name} ({record.file_type.upper()})
Rows: {record.row_count}, Columns: {record.column_count}
Schema: {json.dumps(record.columns_schema, indent=2) if record.columns_schema else 'N/A'}
Statistics: {json.dumps(record.statistics, indent=2) if record.statistics else 'N/A'}
Custom Metrics: {json.dumps(record.custom_metrics) if record.custom_metrics else 'None'}
AI Summary: {record.ai_summary or 'N/A'}"""

    answer = await chat_with_ai(req.question, context)
    return {"answer": answer, "metadata_id": req.metadata_id}
