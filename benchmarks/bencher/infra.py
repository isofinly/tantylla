"""infra.py: OpenTofu lifecycle management.

Wraps tofu apply / destroy / output / workspace so the rest of the framework
needs no knowledge of subprocess plumbing.  tofu stdout/stderr stream directly
to the terminal so the user sees progress in real time.
"""

import json
import subprocess
from pathlib import Path


class TofuError(RuntimeError):
    """Raised when an OpenTofu command exits non-zero."""


def _run(args: list[str], cwd: Path | None = None) -> None:
    """Run a tofu command, streaming output to the terminal."""
    result = subprocess.run(args, cwd=cwd)
    if result.returncode != 0:
        raise TofuError(f"Command failed (exit {result.returncode}): {' '.join(args)}")


def select_workspace(name: str, cwd: Path | None = None) -> None:
    """Select a Terraform workspace, creating it if it does not yet exist."""
    result = subprocess.run(
        ["tofu", "workspace", "select", name],
        cwd=cwd,
        capture_output=True,
    )
    if result.returncode != 0:
        _run(["tofu", "workspace", "new", name], cwd=cwd)


def apply(tfvars: str | Path, cwd: Path | None = None) -> None:
    """Apply the OpenTofu configuration with the given tfvars file."""
    _run(["tofu", "apply", f"-var-file={tfvars}", "--auto-approve"], cwd=cwd)


def destroy(cwd: Path | None = None) -> None:
    """Destroy all resources in the current workspace."""
    _run(["tofu", "destroy", "--auto-approve"], cwd=cwd)


def outputs(cwd: Path | None = None) -> dict:
    """Return current workspace outputs as a plain dict (values unwrapped)."""
    result = subprocess.run(
        ["tofu", "output", "-json"],
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise TofuError(f"tofu output failed: {result.stderr.strip()}")
    # Each entry in the JSON is {"value": ..., "type": ...}; unwrap to bare values.
    return {k: v["value"] for k, v in json.loads(result.stdout).items()}
