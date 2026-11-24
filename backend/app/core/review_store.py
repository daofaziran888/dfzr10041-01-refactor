"""复盘与调研的持久化仓库：JSON 文件落盘，重启后按编号回查。

写盘采用“文件锁 + 重读-修改-写回 + 临时文件原子替换”：
- 同一个状态文件被多个实例（例如重启前后短暂并存、或多个 worker）打开时，
  每次写入都先取锁、重读磁盘、只改自己那一段，避免整份状态互相覆盖；
- 写到一半进程被杀不会留下半截 JSON。
仓库只负责存取，不掺业务判断。
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable

try:  # POSIX 环境有 fcntl；其他环境退化为仅进程内锁
    import fcntl
except ImportError:  # pragma: no cover - 非 POSIX 平台
    fcntl = None  # type: ignore[assignment]

DEFAULT_STATE_PATH = Path(
    os.environ.get("REVIEW_STATE_PATH")
    or (Path(__file__).resolve().parents[2] / "var" / "review_state.json")
)
_SECTIONS = ("archives", "credentials", "originals", "reviews", "items",
             "samples", "dispatches", "events")


def _empty_state() -> dict[str, Any]:
    return {name: {} for name in _SECTIONS}


class ReviewStore:
    """复盘归档、凭证、会话、样本与交付记录的可重启存储。"""

    def __init__(self, path: str | os.PathLike[str] | None = None) -> None:
        self.path = Path(path) if path else DEFAULT_STATE_PATH
        self._lock = threading.RLock()
        self._state: dict[str, Any] = _empty_state()
        self._load()

    # ---------- 读写 ----------

    def _read_file(self) -> dict[str, Any]:
        if not self.path.exists():
            return _empty_state()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            # 损坏的状态文件不能静默覆盖，交给上层换新路径或修数据。
            raise RuntimeError(f"复盘状态文件无法解析：{self.path}") from exc
        state = _empty_state()
        for name in _SECTIONS:
            value = payload.get(name)
            if isinstance(value, dict):
                state[name] = value
        return state

    def _load(self) -> None:
        with self._lock:
            self._state = self._read_file()

    def _flush(self) -> None:
        """原子替换写入；调用方必须已持有文件锁。"""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=self.path.name + ".", suffix=".tmp", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self._state, handle, ensure_ascii=False,
                          sort_keys=True, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self.path)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def _mutate(self, change: Callable[[dict[str, Any]], None]) -> None:
        """在文件锁内重读磁盘、应用变更、原子写回。"""
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            lock_path = self.path.with_name(self.path.name + ".lock")
            with open(lock_path, "a+", encoding="utf-8") as lock_handle:
                if fcntl is not None:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
                try:
                    self._state = self._read_file()
                    change(self._state)
                    self._flush()
                finally:
                    if fcntl is not None:
                        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    def get(self, section: str, key: str) -> Any:
        with self._lock:
            # 读也重读磁盘：别的进程可能刚写过。
            self._state = self._read_file()
            return deepcopy(self._state[section].get(key))

    def put(self, section: str, key: str, value: Any) -> None:
        def apply(state: dict[str, Any]) -> None:
            state[section][key] = deepcopy(value)

        self._mutate(apply)

    def all(self, section: str) -> dict[str, Any]:
        with self._lock:
            self._state = self._read_file()
            return deepcopy(self._state[section])

    def append_event(self, event: dict[str, Any]) -> None:
        """追加审计事件：每次核验/交付/确认都可回查。"""
        def apply(state: dict[str, Any]) -> None:
            state["events"].setdefault("log", []).append(deepcopy(event))

        self._mutate(apply)

    def events(self) -> list[dict[str, Any]]:
        with self._lock:
            self._state = self._read_file()
            return deepcopy(self._state["events"].get("log", []))

    def reset(self) -> None:
        def apply(state: dict[str, Any]) -> None:
            state.clear()
            state.update(_empty_state())

        self._mutate(apply)
