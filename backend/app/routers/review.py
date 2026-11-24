"""历史复盘与调查采样接口：开审、送凭证、交付、确认、续办与回查。

固定路径段（``reviews``/``samples``）必须注册在通用资源路由
``/api/{module}`` 之前，否则会被动态段抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core.review_models import (
    ArchiveBatch, EntryCredential, OriginalRow,
)
from app.core.review_service import ReviewService

router = APIRouter(prefix="/api/reviews", tags=["历史复盘"])


def get_review_service() -> ReviewService:
    """每次请求现取服务，确保重启后读写的是同一份落盘状态。"""
    from app.runtime import review_service
    return review_service


class CredentialPayload(BaseModel):
    credential: dict[str, Any]
    correction: bool = False


class ArchivePayload(BaseModel):
    batch: dict[str, Any]


class ReviewPayload(BaseModel):
    batch_id: str
    credential: dict[str, Any] | None = None
    review_id: str | None = None
    note: str = ""


class OriginalRowPayload(BaseModel):
    row: dict[str, Any]


class AckPayload(BaseModel):
    sample_id: str = Field(min_length=1)


def _guard(review_id: str) -> ReviewService:
    service = get_review_service()
    try:
        service.trace(review_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return service


@router.get("")
def list_reviews() -> dict[str, Any]:
    service = get_review_service()
    return {"items": service.list_reviews(), "events": service.audit_events()}


@router.post("/archives")
def register_archive(payload: ArchivePayload) -> dict[str, Any]:
    """登记归档批次（含每行当时字段与指纹依据）。"""
    service = get_review_service()
    batch = ArchiveBatch.from_dict(payload.batch)
    service.register_archive(batch)
    return {"ok": True, "batch": batch.to_dict()}


@router.post("/originals")
def set_original_row(payload: OriginalRowPayload) -> dict[str, Any]:
    """登记当前实际原行；复盘核验以此为准。"""
    service = get_review_service()
    row = OriginalRow.from_dict(payload.row)
    service.set_original_row(row)
    return {"ok": True, "row": row.to_dict()}


@router.post("")
def start_review(payload: ReviewPayload) -> dict[str, Any]:
    """开立复盘会话；凭证可稍后补送。"""
    service = get_review_service()
    credential = (EntryCredential.from_dict(payload.credential)
                  if payload.credential else None)
    try:
        return service.start_review(payload.batch_id, credential,
                                    review_id=payload.review_id, note=payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/{review_id}")
def trace_review(review_id: str) -> dict[str, Any]:
    return _guard(review_id).trace(review_id)


@router.post("/{review_id}/credentials")
def submit_credential(review_id: str, payload: CredentialPayload) -> dict[str, Any]:
    """补送或更正入口凭证；不成立时保持待核，不猜来源。"""
    service = _guard(review_id)
    credential = EntryCredential.from_dict(payload.credential)
    return service.submit_credential(review_id, credential,
                                     correction=payload.correction)


@router.post("/{review_id}/deliver")
def deliver(review_id: str) -> dict[str, Any]:
    """把已核验原行交付调查采样；已发原行不会重复派出。"""
    return _guard(review_id).deliver_verified(review_id)


@router.post("/{review_id}/resume")
def resume(review_id: str) -> dict[str, Any]:
    """重启后续办：列出未交付条目与游标，不重发已交付样本。"""
    return _guard(review_id).resume_review(review_id)


@router.post("/{review_id}/acknowledge")
def acknowledge(review_id: str, payload: AckPayload) -> dict[str, Any]:
    """复盘端确认样本送达；重复确认幂等。"""
    service = _guard(review_id)
    try:
        return service.acknowledge_sample(payload.sample_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/samples/{review_id}")
def list_samples(review_id: str) -> dict[str, Any]:
    service = _guard(review_id)
    return {"items": service.sampler.list_samples(review_id)}
