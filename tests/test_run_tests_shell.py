from __future__ import annotations

import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


@pytest.mark.skipif(sys.platform != "win32", reason="Windows environment contract")
def test_shell_runner_preserves_native_windows_home_environment(tmp_path: Path) -> None:
    """The hermetic shell runner must retain non-secret Windows home paths."""
    repo_root = Path(__file__).resolve().parent.parent
    script = repo_root / "scripts" / "run_tests.sh"
    git_exe = shutil.which("git")
    if not git_exe:
        pytest.skip("Git for Windows is unavailable")
    git_exec_path = subprocess.run(
        [git_exe, "--exec-path"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if git_exec_path.returncode != 0:
        pytest.skip("Git for Windows is unavailable")
    bash = Path(git_exec_path.stdout.strip()).parents[2] / "bin" / "bash.exe"
    if not bash.exists():
        pytest.skip("Git Bash is unavailable")

    probe = tmp_path / "test_windows_home_probe.py"
    probe.write_text(
        textwrap.dedent(
            """
            import os
            from pathlib import Path

            from hermes_constants import get_hermes_home

            IMPORTED_HOME = Path.home()
            IMPORTED_HERMES_HOME = get_hermes_home()

            def test_native_windows_home_paths_are_available_during_collection():
                assert IMPORTED_HOME == Path(os.environ["USERPROFILE"])
                assert IMPORTED_HERMES_HOME == (
                    Path(os.environ["LOCALAPPDATA"]) / "hermes"
                )
            """
        ),
        encoding="utf-8",
    )

    user_home = tmp_path / "user-home"
    local_appdata = tmp_path / "local-appdata"
    user_home.mkdir()
    local_appdata.mkdir()
    env = os.environ.copy()
    env.update(
        {
            "HOME": str(user_home),
            "USERPROFILE": str(user_home),
            "HOMEDRIVE": user_home.drive,
            "HOMEPATH": str(user_home)[len(user_home.drive) :],
            "LOCALAPPDATA": str(local_appdata),
            "HERMES_PYTHON": sys.executable,
        }
    )

    result = subprocess.run(
        [str(bash), str(script), "-j", "1", str(probe)],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, (result.stdout or "") + (result.stderr or "")
