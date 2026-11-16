"""The Redis layout shared with the gateway (internal/queue/queue.go in the Go code)."""

GROUP = "workers"


def shared_stream(model: str) -> str:
    return f"ig:q:{model}"


def worker_stream(worker: str) -> str:
    return f"ig:w:{worker}"


def live_set(model: str) -> str:
    return f"ig:live:{model}"


def draining_set(model: str) -> str:
    """Workers shutting down: no new traffic, but still watched so a kill mid-drain is rescued."""
    return f"ig:draining:{model}"


def worker_hash(worker: str) -> str:
    return f"ig:worker:{worker}"


def cancel_key(request_id: str) -> str:
    return f"ig:cancel:{request_id}"
