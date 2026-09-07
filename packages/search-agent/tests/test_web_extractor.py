"""Runtime discovery and process-wide warming tests for Defuddle."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from subprocess import CompletedProcess
from threading import Event, Lock
from unittest.mock import patch

from search_agent.web.extractor import (
    NpxRuntime,
    _discover_npx_runtime,
    _reset_defuddle_bootstrap_for_tests,
    build_local_defuddle_extractor,
)


def test_runtime_discovery_prefers_direct_node_and_npx() -> None:
    """Avoid fnm indirection when both executables are already on PATH."""

    paths = {"node": "/runtime/bin/node", "npx": "/runtime/bin/npx"}
    with patch(
        "search_agent.web.extractor.shutil.which", side_effect=paths.get
    ) as which:
        runtime, error = _discover_npx_runtime()

    assert runtime == NpxRuntime(("/runtime/bin/npx",), "PATH (/runtime/bin/node)")
    assert error is None
    assert which.call_count == 2


def test_runtime_discovery_uses_installed_fnm_default_without_shell_eval() -> None:
    """Build an argv-only fnm prefix without downloading or changing Node."""

    paths = {"node": None, "npx": None, "fnm": "/tools/fnm"}
    completed = CompletedProcess(
        args=["fnm", "default"], returncode=0, stdout="v20.19.4\n", stderr=""
    )
    with (
        patch("search_agent.web.extractor.shutil.which", side_effect=paths.get),
        patch("search_agent.web.extractor.subprocess.run", return_value=completed),
    ):
        runtime, error = _discover_npx_runtime()

    assert runtime == NpxRuntime(
        ("/tools/fnm", "exec", "--using=v20.19.4", "--", "npx"),
        "fnm v20.19.4",
    )
    assert error is None


def test_defuddle_warming_is_single_flight_across_threads() -> None:
    """Concurrent contexts share one completed bootstrap operation."""

    _reset_defuddle_bootstrap_for_tests()
    entered = Event()
    release = Event()
    count_lock = Lock()
    call_count = 0
    expected = (None, "fixture unavailable")

    def select_once():
        nonlocal call_count
        with count_lock:
            call_count += 1
        entered.set()
        assert release.wait(timeout=2), "test did not release bootstrap"
        return expected

    try:
        with patch(
            "search_agent.web.extractor._select_and_warm_local_extractor",
            side_effect=select_once,
        ):
            with ThreadPoolExecutor(max_workers=6) as pool:
                futures = [
                    pool.submit(build_local_defuddle_extractor) for _ in range(6)
                ]
                assert entered.wait(timeout=2), "bootstrap did not start"
                release.set()
                results = [future.result(timeout=2) for future in futures]
        assert call_count == 1
        assert results == [expected] * 6
    finally:
        _reset_defuddle_bootstrap_for_tests()
