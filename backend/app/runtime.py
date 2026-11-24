"""运行期单例：复盘仓库与调查采样在同一进程内共享一份落盘状态。

状态文件路径由 ``REVIEW_STATE_PATH`` 覆盖，默认写到 ``backend/var/``；
进程重启后自动从该文件恢复，接口按复盘编号即可回查。
"""
from __future__ import annotations

from app.core.review_service import InvestigationSampler, ReviewService
from app.core.review_store import DEFAULT_STATE_PATH, ReviewStore

review_store = ReviewStore(DEFAULT_STATE_PATH)
investigation_sampler = InvestigationSampler(review_store)
review_service = ReviewService(review_store, investigation_sampler)
