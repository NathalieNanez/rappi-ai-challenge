from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, List, Optional

from app.bot.agent import RappiAgent, get_or_create_session, clear_session
from app.insights.engine import InsightEngine, Insight
from app.data.loader import get_loader

app = FastAPI(
    title="Rappi Data Intelligence API",
    description="API for the Rappi SP&A Operations Assistant",
    version="1.0.0",
)

# Initialize singletons on startup
agent = RappiAgent()
engine = InsightEngine()


class ChatRequest(BaseModel):
    session_id: str
    message: str


class ChatResponse(BaseModel):
    text: str
    charts: List[str]
    followups: List[str]
    intent: str
    cost: dict


@app.post("/api/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    """
    Process a chat message using the ReAct agent.
    Maintains memory and cost tracking per session_id.
    """
    try:
        session = get_or_create_session(request.session_id)
        result = agent.chat(session, request.message)
        return ChatResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/chat/{session_id}")
async def clear_chat_session(session_id: str):
    """Clear memory and reset the cost monitor for a given session."""
    clear_session(session_id)
    return {"status": "cleared", "session_id": session_id}


@app.get("/api/insights", response_model=List[Insight])
async def get_insights():
    """Run the insight engine and return structured findings."""
    try:
        insights = engine.get_all_insights()
        return insights
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/health")
async def health_check():
    return {"status": "ok"}
