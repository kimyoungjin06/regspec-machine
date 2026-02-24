from __future__ import annotations

import subprocess
import sys


def test_package_import_does_not_preload_launcher_module() -> None:
    code = (
        "import sys\n"
        "import regspec_machine\n"
        "print('regspec_machine.launcher' in sys.modules)\n"
        "getattr(regspec_machine, 'parse_args')\n"
        "print('regspec_machine.launcher' in sys.modules)\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    lines = out.splitlines()
    assert lines == ["False", "True"]
