"""
DataHub - Module 5: Metadata Extraction & Indexing
FastAPI Application Entry Point
"""
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db.database import engine, Base
from app.routers import auth, metadata, projects, ai_router
from app.config import get_settings

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create all tables on startup
    Base.metadata.create_all(bind=engine)
    os.makedirs(settings.upload_dir, exist_ok=True)
    yield


app = FastAPI(
    title="DataHub — Module 5: Metadata Extraction & Indexing",
    description=(
        "Content-Addressable Version Control System for large-scale data lineage. "
        "Module 5: Automated metadata extraction from CSV/JSON/Parquet files on commit."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow frontend origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173", "null", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(auth.router)
app.include_router(metadata.router)
app.include_router(projects.router)
app.include_router(ai_router.router)


@app.get("/", tags=["Root"])
def root():
    return {
        "message": "DataHub Module 5 API — Metadata Extraction & Indexing Engine",
        "version": "1.0.0",
        "docs": "/docs",
        "module": "Module 5",
    }


@app.get("/health", tags=["Root"])
def health():
    return {"status": "ok"}
