"""File-backed persistent state for the Loop Controller."""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path

from automation.loop_controller.schemas import LoopState

_LOCK = threading.Lock()


def _default_state_path() -> Path:
    repo = Path(os.getenv("LOOP_CONTROLLER_REPO_ROOT", "")).resolve() or Path(__file__).resolve().parents[2]
    return repo / "runtime" / "loop_controller" / "state.json"


class StateStore:
    """Thread-safe, file-backed LoopState persistence."""

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or _default_state_path()
        self._state: LoopState | None = None

    @property
    def path(self) -> Path:
        return self._path

    # -- read ---------------------------------------------------------------

    def load(self) -> LoopState:
        with _LOCK:
            if self._path.exists():
                try:
                    data = json.loads(self._path.read_text("utf-8"))
                    self._state = LoopState.model_validate(data)
                except (json.JSONDecodeError, Exception):
                    self._state = LoopState()
            else:
                self._state = LoopState()
            return self._state.model_copy(deep=True)

    def get(self) -> LoopState:
        if self._state is None:
            return self.load()
        with _LOCK:
            return self._state.model_copy(deep=True)

    # -- write --------------------------------------------------------------

    def save(self, state: LoopState) -> None:
        with _LOCK:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(state.model_dump(mode="json"), indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            tmp.replace(self._path)
            self._state = state.model_copy(deep=True)

    def update(self, **kwargs: object) -> LoopState:
        """Load current state, apply partial updates, save and return."""
        state = self.load()
        for key, value in kwargs.items():
            if hasattr(state, key):
                setattr(state, key, value)
        self.save(state)
        return state

    # -- truncation helpers -------------------------------------------------

    def trim_history(self, max_rounds: int = 200) -> None:
        state = self.load()
        if len(state.round_history) > max_rounds:
            state.round_history = state.round_history[-max_rounds:]
            self.save(state)
