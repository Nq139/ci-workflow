"""LPSS 后台节点的生命周期管理。"""

import atexit
import os
import re
import signal
import subprocess
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from threading import Condition, Lock, Thread
from typing import List, Optional, Union

from framework import logger
from framework.exceptions import KeywordSkipped


class LpssAdapter(Enum):
    """可由 KDT 启动和终止的 LPSS Adapter。"""

    MSG_STRPUB = "lpss.msg.strpub"
    MSG_STRSUB = "lpss.msg.strsub"
    MSG_TESTPUB = "lpss.msg.testpub"
    MSG_TESTSUB = "lpss.msg.testsub"
    RSD2_ACCEPTANCE = "lpss.rsd2.acceptance"
    SRV_CLIENT = "lpss.srv.client"
    SRV_SERVICE = "lpss.srv.service"
    SRV_TESTCLIENT = "lpss.srv.testclient"
    SRV_TESTSERVICE = "lpss.srv.testservice"


class LpssQueryAdapter(Enum):
    """短生命周期的 LPSS 查询工具，不属于可停止的后台节点。"""

    TOOL = "lpss.tool"


class LpssSignal(Enum):
    """可发送给 LPSS 后台节点的终止信号。"""

    SIGINT = "SIGINT"
    SIGTERM = "SIGTERM"
    SIGABRT = "SIGABRT"
    SIGSEGV = "SIGSEGV"
    SIGKILL = "SIGKILL"


@dataclass(eq=False)
class LpssTask:
    """由 KDT 启动并可通过步骤结果引用的 LPSS 后台任务。"""

    adapter: LpssAdapter
    node_name: str
    process: subprocess.Popen
    collect_results: bool = False
    results: List[str] = field(default_factory=list)
    finished: bool = False
    condition: Condition = field(default_factory=Condition, repr=False)


_BACKGROUND_PROCESSES: List[LpssTask] = []
_BACKGROUND_PROCESSES_LOCK = Lock()
_STOP_REQUESTED = set()
_SHUTTING_DOWN = False
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_SKIPPED_EXIT_CODE = 77
_RESULT_PREFIX = "KDT_RESULT: "


def _adapter(adapter: Union[LpssAdapter, LpssQueryAdapter]) -> Path:
    """返回 adapter/bin 下指定 LPSS 可执行程序的绝对路径。"""
    suffix = ".exe" if os.name == "nt" else ""
    executable = (
        Path(__file__).resolve().parents[3]
        / "adapter"
        / "bin"
        / f"{adapter.value}{suffix}"
    )
    if not executable.is_file():
        raise FileNotFoundError(f"Adapter executable not found: {executable}")
    return executable


def _stream_adapter_output(task: LpssTask, stream):
    """将后台 Adapter 的输出实时转发到 KDT 日志。"""
    if stream is None:
        return
    try:
        for line in stream:
            message = _ANSI_ESCAPE.sub("", line.rstrip("\r\n"))
            if message.startswith(_RESULT_PREFIX):
                if task.collect_results:
                    result = message[len(_RESULT_PREFIX):]
                    with task.condition:
                        task.results.append(result)
                        task.condition.notify_all()
                continue
            logger.info(message, source=task.adapter.value)
    finally:
        stream.close()


def _monitor_adapter(task: LpssTask):
    """转发后台 Adapter 输出，等待进程结束并维护进程表。"""
    process = task.process
    _stream_adapter_output(task, process.stdout)
    process.wait()

    with task.condition:
        task.finished = True
        task.condition.notify_all()

    with _BACKGROUND_PROCESSES_LOCK:
        stopped = process in _STOP_REQUESTED
        _STOP_REQUESTED.discard(process)
        _BACKGROUND_PROCESSES[:] = [
            item for item in _BACKGROUND_PROCESSES if item.process is not process
        ]

    if not _SHUTTING_DOWN and process.returncode != 0 and not stopped:
        logger.error(
            f"Process exited with code {process.returncode}",
            source=task.adapter.value,
        )


def _start(
    adapter: LpssAdapter,
    arguments: List[str],
    collect_results: bool = False,
):
    """启动并跟踪一个长生命周期 LPSS Adapter，返回任务句柄。"""
    if not arguments:
        raise ValueError("LPSS Adapter arguments must contain a node name")

    node_name = arguments[0]
    process_options = {}
    if os.name == "nt":
        process_options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        process_options["start_new_session"] = True

    process = subprocess.Popen(
        [str(_adapter(adapter)), *arguments],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        **process_options,
    )
    task = LpssTask(adapter, node_name, process, collect_results=collect_results)
    with _BACKGROUND_PROCESSES_LOCK:
        _BACKGROUND_PROCESSES.append(task)

    Thread(target=_monitor_adapter, args=(task,), daemon=True).start()
    return task


def result(task: LpssTask, count: int, unique: bool, timeout: float):
    """
    等待 LPSS 后台任务产生指定数量的结果并返回。

    ``unique`` 为 ``True`` 时按不同结果的数量等待并返回排序后的列表，
    否则按接收顺序计数。仅等待一个结果时直接返回字符串。

    :param task: LPSS 后台任务句柄
    :param count: 等待的结果数量
    :param unique: 是否按不同结果计数
    :param timeout: 最长等待时间，单位为秒
    """
    if not isinstance(task, LpssTask):
        raise TypeError("task must be an LPSS background task result")
    if count <= 0:
        raise ValueError("count must be greater than zero")
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero")

    deadline = time.monotonic() + timeout
    with task.condition:
        while True:
            values = list(dict.fromkeys(task.results)) if unique else list(task.results)
            if len(values) >= count:
                selected = sorted(values) if unique else values[:count]
                return selected[0] if count == 1 else selected
            if task.finished:
                raise RuntimeError(
                    f"{task.adapter.value} node {task.node_name!r} exited after "
                    f"producing {len(values)} of {count} required result(s)"
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Timed out after {timeout:g}s waiting for {count} "
                    f"result(s) from {task.adapter.value} node {task.node_name!r}; "
                    f"received {values!r}"
                )
            task.condition.wait(remaining)


def _run(adapter: Union[LpssAdapter, LpssQueryAdapter], arguments: List[str], expected_exit_code: int = 0, timeout: Optional[float] = None):
    """执行短生命周期 LPSS Adapter，并校验退出状态和最长运行时间。"""
    result = subprocess.run(
        [str(_adapter(adapter)), *arguments],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )
    for line in result.stdout.splitlines():
        logger.info(_ANSI_ESCAPE.sub("", line), source=adapter.value)
    if result.returncode == _SKIPPED_EXIT_CODE:
        raise KeywordSkipped(
            result.stdout.strip() or f"{adapter.value} is not supported"
        )
    if result.returncode != expected_exit_code:
        raise RuntimeError(
            f"{adapter.value} exited with code {result.returncode}, "
            f"expected {expected_exit_code}"
        )

    return result.stdout


def _require_supported(adapter: LpssAdapter):
    """查询 Adapter 能力，不支持时通知 Runner 跳过当前关键字。"""
    result = subprocess.run(
        [str(_adapter(adapter)), "--kdt-capability"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode == _SKIPPED_EXIT_CODE:
        raise KeywordSkipped(
            result.stdout.strip() or f"{adapter.value} requires C++20"
        )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to query {adapter.value} capability: "
            f"exit code {result.returncode}"
        )


def _native_signal(signal_type: LpssSignal) -> int:
    """返回当前平台对应的原生信号值。"""
    native_signal = getattr(signal, signal_type.value, None)
    if native_signal is None:
        raise RuntimeError(
            f"Signal {signal_type.value} is not supported on this platform"
        )
    return native_signal


def _send_process_signal(
    process: subprocess.Popen, signal_type: LpssSignal
):
    """向指定后台 Adapter 发送所选信号。"""
    if os.name == "nt":
        if signal_type is LpssSignal.SIGTERM:
            # CTRL_BREAK_EVENT 可定向到指定进程组，RMVL 将其作为 SIGTERM 交给节点执行优雅退出。
            process.send_signal(signal.CTRL_BREAK_EVENT)
        elif signal_type is LpssSignal.SIGKILL:
            process.kill()
        elif signal_type is LpssSignal.SIGINT:
            raise RuntimeError(
                "SIGINT cannot be reliably directed to one process group on Windows; use SIGTERM"
            )
        else:
            process.send_signal(_native_signal(signal_type))
        return

    process.send_signal(_native_signal(signal_type))


def _request_process_stop(process: subprocess.Popen, signal_type: LpssSignal = LpssSignal.SIGTERM) -> bool:
    """向后台 Adapter 发送信号，返回超时后是否使用了强制终止。"""
    if process.poll() is not None:
        return False
    try:
        _send_process_signal(process, signal_type)
    except (OSError, ValueError):
        process.kill()
        process.wait()
        return True

    deadline = time.monotonic() + 1
    while process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.02)
    if process.poll() is None:
        process.kill()
        process.wait()
        return True
    return False


def stop(adapter: LpssAdapter, signal_type: LpssSignal, node_name: str):
    """
    向指定名称的 LPSS 后台节点发送信号。

    SIGINT 和 SIGTERM 要求节点在超时时间内以退出码 0 优雅退出；其他信号
    要求节点确实由所选信号终止。Windows 无法向指定进程组可靠发送 SIGINT，
    应使用 SIGTERM。若存在多个同名同类型节点，则全部终止并逐一校验。

    :param adapter: 待停止节点使用的 LPSS Adapter
    :param signal_type: 要发送的信号类型
    :param node_name: 启动组件传入的节点名称
    """
    if os.name == "nt" and signal_type is LpssSignal.SIGINT:
        raise RuntimeError(
            "SIGINT cannot be reliably directed to one process group on Windows; use SIGTERM"
        )

    with _BACKGROUND_PROCESSES_LOCK:
        tasks = [
            task
            for task in _BACKGROUND_PROCESSES
            if task.adapter is adapter and task.node_name == node_name
        ]
        processes = [task.process for task in tasks]
        _STOP_REQUESTED.update(processes)

    if not processes:
        if adapter in {
            LpssAdapter.SRV_CLIENT,
            LpssAdapter.SRV_SERVICE,
            LpssAdapter.SRV_TESTCLIENT,
            LpssAdapter.SRV_TESTSERVICE,
        }:
            _require_supported(adapter)
        raise RuntimeError(f"No {adapter.value} process named {node_name!r} is running")

    failures = []
    for process in processes:
        forced = _request_process_stop(process, signal_type)
        if forced:
            failures.append(f"PID {process.pid} did not exit after {signal_type.value}")
        elif signal_type in {
            LpssSignal.SIGINT,
            LpssSignal.SIGTERM,
        } and process.returncode != 0:
            failures.append(f"PID {process.pid} exited with code {process.returncode}")
        elif os.name != "nt" and signal_type not in {
            LpssSignal.SIGINT,
            LpssSignal.SIGTERM,
        }:
            expected_code = -int(_native_signal(signal_type))
            if process.returncode != expected_code:
                failures.append(
                    f"PID {process.pid} exited with code {process.returncode}, expected {expected_code} for {signal_type.value}"
                )

    if failures:
        raise RuntimeError(
            f"Failed to stop {adapter.value} node {node_name!r}: "
            + "; ".join(failures)
        )

    logger.success(
        f"Sent {signal_type.value} to {len(processes)} "
        f"{adapter.value} process(es) named {node_name!r}"
    )


def _cleanup_background_processes():
    """KDT 进程退出时终止仍在运行的 LPSS Adapter。"""
    global _SHUTTING_DOWN
    _SHUTTING_DOWN = True
    with _BACKGROUND_PROCESSES_LOCK:
        processes = [task.process for task in _BACKGROUND_PROCESSES]
        _STOP_REQUESTED.update(processes)
    for process in processes:
        _request_process_stop(process)


atexit.register(_cleanup_background_processes)
