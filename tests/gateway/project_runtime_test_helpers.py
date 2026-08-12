"""Deterministic subprocess synchronization for Task-7 runtime tests."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import functools
import json
import os
import queue
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Callable


REPO_ROOT = Path(__file__).resolve().parents[2]


class ManualMonotonicClock:
    """Exact manually advanced clock for event-gated runtime-loop tests."""

    def __init__(self, value: float = 0.0) -> None:
        self.value = value
        self.reads: list[float] = []

    def __call__(self) -> float:
        self.reads.append(self.value)
        return self.value

    def advance_to(self, value: float) -> None:
        if value < self.value:
            raise ValueError("manual clock cannot move backwards")
        self.value = value


class WakeWaitProbe:
    """Observe each dispatcher wait and release it only through its wake."""

    def __init__(self) -> None:
        self.calls: list[tuple[asyncio.Event, float]] = []
        self.entered: asyncio.Queue[int] = asyncio.Queue()

    async def __call__(
        self,
        wake_event: asyncio.Event,
        timeout_seconds: float,
    ) -> str:
        call_index = len(self.calls)
        self.calls.append((wake_event, timeout_seconds))
        self.entered.put_nowait(call_index)
        await wake_event.wait()
        return "wake"

    async def next_wait(self) -> int:
        return await asyncio.wait_for(self.entered.get(), timeout=5)


class DeadlineWakeWaitProbe:
    """Release a dispatcher wait through either its wake or a fake deadline."""

    def __init__(self) -> None:
        self.calls: list[tuple[asyncio.Event, float, asyncio.Event]] = []
        self.entered: asyncio.Queue[int] = asyncio.Queue()
        self.release_reasons: list[tuple[int, str]] = []

    async def __call__(
        self,
        wake_event: asyncio.Event,
        timeout_seconds: float,
    ) -> str:
        call_index = len(self.calls)
        deadline = asyncio.Event()
        self.calls.append((wake_event, timeout_seconds, deadline))
        self.entered.put_nowait(call_index)
        wake_task = asyncio.create_task(wake_event.wait())
        deadline_task = asyncio.create_task(deadline.wait())
        done, pending = await asyncio.wait(
            (wake_task, deadline_task),
            return_when=asyncio.FIRST_COMPLETED,
        )
        reason = "wake" if wake_task in done else "timeout"
        self.release_reasons.append((call_index, reason))
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        return reason

    async def next_wait(self) -> int:
        return await asyncio.wait_for(self.entered.get(), timeout=5)

    def expire(self, call_index: int) -> None:
        wake_event, _, deadline = self.calls[call_index]
        assert not wake_event.is_set()
        deadline.set()


class RetainedThreadRunner:
    """Dedicated off-loop runner that joins its exact call on cancellation."""

    def __init__(self, name: str, *, max_workers: int = 1) -> None:
        self.name = name
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=name,
        )
        self.calls: list[tuple[int, str]] = []
        self.futures: list[asyncio.Future[Any]] = []

    async def __call__(
        self,
        function: Callable[..., Any],
        /,
        *args: object,
        **kwargs: object,
    ) -> Any:
        loop = asyncio.get_running_loop()
        context = contextvars.copy_context()
        call = functools.partial(function, *args, **kwargs)

        def invoke() -> Any:
            self.calls.append(
                (
                    threading.get_ident(),
                    getattr(function, "__name__", type(function).__name__),
                )
            )
            return context.run(call)

        future = loop.run_in_executor(self.executor, invoke)
        self.futures.append(future)
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            try:
                await asyncio.shield(future)
            except BaseException:
                pass
            raise

    def close(self) -> None:
        assert all(future.done() for future in self.futures)
        self.executor.shutdown(wait=True, cancel_futures=False)


class ProbeHandle:
    """One JSON-lines child controlled through explicit ready/go events."""

    def __init__(self, process: subprocess.Popen[str], probe_id: str):
        self.process = process
        self.probe_id = probe_id
        self.stdout: queue.Queue[str | None] = queue.Queue()
        self.stderr: list[str] = []
        self.threads = (
            threading.Thread(
                target=self._pump_stdout,
                name=f"task7-probe-stdout-{probe_id}",
                daemon=True,
            ),
            threading.Thread(
                target=self._pump_stderr,
                name=f"task7-probe-stderr-{probe_id}",
                daemon=True,
            ),
        )
        for thread in self.threads:
            thread.start()

    def _pump_stdout(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.stdout.put(line)
        self.stdout.put(None)

    def _pump_stderr(self) -> None:
        assert self.process.stderr is not None
        self.stderr.extend(self.process.stderr)

    def send(self, payload: dict[str, object]) -> None:
        assert self.process.stdin is not None
        self.process.stdin.write(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )
        self.process.stdin.flush()

    def expect(
        self, event: str, *, timeout: float = 15
    ) -> dict[str, object]:
        try:
            line = self.stdout.get(timeout=timeout)
        except queue.Empty as exc:
            raise AssertionError(
                f"probe {self.probe_id} timed out before {event}; "
                f"stderr={''.join(self.stderr)!r}"
            ) from exc
        if line is None:
            raise AssertionError(
                f"probe {self.probe_id} exited before {event}; "
                f"returncode={self.process.poll()}; "
                f"stderr={''.join(self.stderr)!r}"
            )
        payload = json.loads(line)
        assert payload["version"] == 1
        assert payload["probe_id"] == self.probe_id
        assert payload["event"] == event
        return payload

    def complete(
        self, *, returncode: int = 0, timeout: float = 15
    ) -> None:
        actual = self.process.wait(timeout=timeout)
        for thread in self.threads:
            thread.join(timeout=5)
            assert not thread.is_alive()
        extras: list[str] = []
        while True:
            try:
                line = self.stdout.get_nowait()
            except queue.Empty:
                break
            if line is not None:
                extras.append(line)
        assert actual == returncode, (
            self.probe_id,
            actual,
            "".join(self.stderr),
        )
        assert extras == []


class ProbeSet:
    """Own and clean every child even when one assertion fails."""

    def __init__(self, script: Path):
        self._script = script
        self.processes: list[subprocess.Popen[str]] = []
        self.handles: list[ProbeHandle] = []

    def __enter__(self) -> ProbeSet:
        return self

    def spawn(self, prepare: dict[str, object]) -> ProbeHandle:
        environment = os.environ.copy()
        environment["PYTHONPATH"] = os.pathsep.join(
            filter(
                None,
                (
                    str(REPO_ROOT),
                    environment.get("PYTHONPATH"),
                ),
            )
        )
        environment["PYTHONUTF8"] = "1"
        environment["PYTHONIOENCODING"] = "utf-8"
        process = subprocess.Popen(
            [sys.executable, str(self._script)],
            cwd=REPO_ROOT,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        self.processes.append(process)
        handle = ProbeHandle(
            process,
            str(prepare["probe_id"]),
        )
        self.handles.append(handle)
        handle.send(prepare)
        return handle

    def __exit__(self, exc_type, exc, traceback) -> bool:
        cleanup_errors: list[str] = []

        for index, process in enumerate(self.processes):
            try:
                if process.poll() is None:
                    process.terminate()
            except BaseException as cleanup_error:
                cleanup_errors.append(
                    f"process {index} terminate: {cleanup_error!r}"
                )
        for index, process in enumerate(self.processes):
            try:
                if process.poll() is None:
                    process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    process.kill()
                    process.wait(timeout=5)
                except BaseException as cleanup_error:
                    cleanup_errors.append(
                        f"process {index} kill: {cleanup_error!r}"
                    )
            except BaseException as cleanup_error:
                cleanup_errors.append(
                    f"process {index} wait: {cleanup_error!r}"
                )
        for process_index, process in enumerate(self.processes):
            for stream in (
                process.stdin,
                process.stdout,
                process.stderr,
            ):
                if stream is None:
                    continue
                try:
                    stream.close()
                except BaseException as cleanup_error:
                    cleanup_errors.append(
                        "process "
                        f"{process_index} stream close: {cleanup_error!r}"
                    )
        for handle_index, handle in enumerate(self.handles):
            for thread_index, thread in enumerate(handle.threads):
                thread.join(timeout=5)
                if thread.is_alive():
                    cleanup_errors.append(
                        "handle "
                        f"{handle_index} thread {thread_index} is alive"
                    )
        if cleanup_errors:
            message = "probe cleanup failed: " + "; ".join(
                cleanup_errors
            )
            if exc is not None:
                exc.add_note(message)
            else:
                raise AssertionError(message)
        return False


def release_probes(handles: list[ProbeHandle]) -> None:
    """Release contenders together only after every child reached its gate."""
    for handle in handles:
        ready = handle.expect("ready")
        assert ready["stage"] == "before_begin_immediate"
    barrier = threading.Barrier(len(handles) + 1)
    errors: list[BaseException] = []

    def release(handle: ProbeHandle) -> None:
        try:
            barrier.wait(timeout=5)
            handle.send(
                {
                    "version": 1,
                    "event": "go",
                    "probe_id": handle.probe_id,
                }
            )
        except BaseException as error:
            errors.append(error)

    threads = [
        threading.Thread(
            target=release,
            args=(handle,),
            daemon=True,
        )
        for handle in handles
    ]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=5)
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()
    assert errors == []


def run_probe(
    probes: ProbeSet,
    prepare: dict[str, object],
) -> dict[str, object]:
    handle = probes.spawn(prepare)
    release_probes([handle])
    payload = handle.expect("result")
    handle.complete()
    return payload
