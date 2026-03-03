from collections import deque
from concurrent.futures import Executor, Future
from dataclasses import dataclass, field
from threading import Lock

@dataclass
class ExecutorState:
    executor: Executor
    futures: dict[str, Future] = field(default_factory=dict)
    completed_ids: deque[str] = field(default_factory=deque)
    lock: Lock = field(default_factory=Lock)


_executor_states: dict[str, ExecutorState] = {}
_registry_lock = Lock()


def get_executor_state(
    name: str, executor_class: type[Executor], max_workers: int
) -> ExecutorState:
    with _registry_lock:
        if name not in _executor_states:
            _executor_states[name] = ExecutorState(
                executor=executor_class(max_workers=max_workers),
            )
        return _executor_states[name]


def shutdown_executor(name: str, wait: bool = True) -> None:
    with _registry_lock:
        state = _executor_states.pop(name, None)
        if state:
            state.executor.shutdown(wait=wait)
