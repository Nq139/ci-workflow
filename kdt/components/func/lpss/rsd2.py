"""RSD2 端点生命周期验收组件。"""

from .base import LpssAdapter, _request_process_stop, _start, result


def acceptance(mode: str, cycles: int = 200):
    """启动两个独立节点，逐轮验证端点上线和下线。"""
    if mode not in ("sync", "async") or type(cycles) is not int or not 1 <= cycles <= 200:
        raise ValueError("mode must be sync|async; cycles must be 1..200")

    tasks = []
    try:
        for role in ("A", "B"):
            tasks.append(_start(
                LpssAdapter.RSD2_ACCEPTANCE, [role, mode, str(cycles)],
                collect_results=True,
            ))

        for role, task in zip(("A", "B"), tasks):
            expected = f"PASS COMPLETE {role} {mode} {cycles}"
            observed = result(task, 1, False, 180)
            if observed != expected:
                raise AssertionError(f"RSD2 {role}: expected {expected!r}, got {observed!r}")
            task.process.wait(timeout=10)
            if task.process.returncode != 0:
                raise RuntimeError(f"RSD2 {role} exited with {task.process.returncode}")
    finally:
        for task in tasks:
            if task.process.poll() is None:
                _request_process_stop(task.process)
