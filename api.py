#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI compute engine for the AI Code Assistant.

Exposes two endpoints:
  POST /evaluate_fame   – Tier 1 deterministic FAME-to-Python conversion.
  POST /log_conversion  – Audit log for completed conversions.

The FAME parser is provided by the `fame2pygen` package (separate local
repository).  If that package is not installed the app raises a clear
startup error rather than silently returning wrong results at runtime.
"""

from __future__ import annotations

import itertools
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Attempt to import the FAME parser from the sibling Fame2PyGen repository.
# A missing package surfaces immediately so the operator knows to install it.
# ---------------------------------------------------------------------------
try:
    from fame2pygen import parse_fame_formula  # type: ignore[import]

    _PARSER_AVAILABLE = True
except ImportError:  # pragma: no cover – guarded at startup in production
    _PARSER_AVAILABLE = False
    parse_fame_formula = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Application setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="AI Code Assistant – Compute Engine",
    description=(
        "Deterministic FAME-to-Python conversion (Tier 1) "
        "powered by the Fame2PyGen parser."
    ),
    version="1.0.0",
)

logger = logging.getLogger("seriesvault.api")

# In-memory audit log (replace with a persistent store in production)
_conversion_log: list[dict[str, Any]] = []
_entry_id_counter: itertools.count[int] = itertools.count()

# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------


class FameEvaluationRequest(BaseModel):
    """Input payload for /evaluate_fame."""

    fame_code: str


class FameEvaluationResponse(BaseModel):
    """Successful Tier 1 conversion response."""

    tier: int
    confidence: str
    python_code: str


class ManualReviewResponse(BaseModel):
    """Response returned when the formula contains complex / unsupported functions."""

    tier: int
    confidence: str
    message: str
    flagged_functions: list[str]


class ConversionLogEntry(BaseModel):
    """Payload for the /log_conversion audit endpoint."""

    fame_code: str
    python_code: str | None = None
    tier: int
    confidence: str
    notes: str | None = None


class ConversionLogResponse(BaseModel):
    """Confirmation returned after a log entry is stored."""

    status: str
    logged_at: str
    entry_id: int


# ---------------------------------------------------------------------------
# Startup guard
# ---------------------------------------------------------------------------


@app.on_event("startup")
async def _check_parser() -> None:
    """Fail fast if the fame2pygen package is not installed."""
    if not _PARSER_AVAILABLE:
        logger.error(
            "fame2pygen package is not installed. "
            "Install it from the Fame2PyGen local repository before starting the server."
        )
        raise RuntimeError(
            "fame2pygen is not installed. "
            "Run `pip install -e /path/to/Fame2PyGen` and restart."
        )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.post(
    "/evaluate_fame",
    response_model=FameEvaluationResponse | ManualReviewResponse,
    summary="Tier 1 deterministic FAME-to-Python conversion",
    tags=["conversion"],
)
async def evaluate_fame(request: FameEvaluationRequest) -> dict[str, Any]:
    """
    Parse a FAME formula and attempt a fully automatic Python translation.

    * If the parser maps every construct deterministically (no complex
      functions flagged for manual review) the endpoint returns Tier 1
      output with ``confidence = "high"`` and the translated Python code.

    * If the parser detects constructs that require human review it returns
      a Tier 2 response indicating which functions need attention.
    """
    if not request.fame_code.strip():
        raise HTTPException(status_code=422, detail="fame_code must not be empty.")

    try:
        result = parse_fame_formula(request.fame_code)
    except Exception as exc:
        logger.exception("Parser raised an unexpected error for input: %r", request.fame_code)
        raise HTTPException(
            status_code=500,
            detail=f"Parser error: {exc}",
        ) from exc

    # The parser contract:
    #   result["python_code"]        – translated code (str)
    #   result["requires_review"]    – True when complex functions were found
    #   result["flagged_functions"]  – list[str] of function names (may be empty)
    python_code: str = result.get("python_code", "")
    requires_review: bool = bool(result.get("requires_review", False))
    flagged_functions: list[str] = result.get("flagged_functions", [])

    if not requires_review:
        return {
            "tier": 1,
            "confidence": "high",
            "python_code": python_code,
        }

    # Complex functions detected – cannot guarantee automatic conversion
    return {
        "tier": 2,
        "confidence": "low",
        "message": (
            "Formula contains functions marked for manual review. "
            "Automatic conversion is not possible for these constructs."
        ),
        "flagged_functions": flagged_functions,
    }


@app.post(
    "/log_conversion",
    response_model=ConversionLogResponse,
    summary="Audit log for FAME-to-Python conversions",
    tags=["audit"],
)
async def log_conversion(entry: ConversionLogEntry) -> dict[str, Any]:
    """
    Persist a conversion audit record.

    Each call appends one entry to the in-memory audit log and returns its
    assigned ``entry_id`` together with the server-side timestamp.  Swap the
    in-memory list for a database connection in production deployments.
    """
    logged_at = datetime.now(tz=timezone.utc).isoformat()

    record: dict[str, Any] = {
        "entry_id": next(_entry_id_counter),
        "logged_at": logged_at,
        "fame_code": entry.fame_code,
        "python_code": entry.python_code,
        "tier": entry.tier,
        "confidence": entry.confidence,
        "notes": entry.notes,
    }

    _conversion_log.append(record)
    logger.info("Logged conversion entry #%d at %s", record["entry_id"], logged_at)

    return {
        "status": "logged",
        "logged_at": logged_at,
        "entry_id": record["entry_id"],
    }
