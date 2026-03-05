"""
Google Colab helper for cloning, installing, importing, and running Document Refinery.

Usage in Colab:
    from colab_runner import setup_and_run
    results = setup_and_run(
        repo_url="https://github.com/<user>/document-refinery.git",
        repo_dir="/content/document-refinery",
        pdf_folder="data/raw",
        workers=1,
        confidence_threshold=0.7,
    )
"""

from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional


def _run(cmd: str, cwd: Optional[str] = None) -> None:
    """Run a shell command and stream output."""
    print(f"$ {cmd}")
    subprocess.run(cmd, cwd=cwd, shell=True, check=True)


def clone_or_update(repo_url: str, repo_dir: str = "/content/document-refinery") -> str:
    """Clone repo if absent, otherwise pull latest."""
    repo_path = Path(repo_dir)
    parent = repo_path.parent
    parent.mkdir(parents=True, exist_ok=True)

    if (repo_path / ".git").exists():
        _run("git pull", cwd=str(repo_path))
    else:
        _run(f"git clone {shlex.quote(repo_url)} {shlex.quote(str(repo_path))}")
    return str(repo_path)


def _relax_python_requirement_for_colab(repo_dir: str) -> None:
    """Patch pyproject locally for Colab Python (usually 3.12)."""
    pyproject = Path(repo_dir) / "pyproject.toml"
    if not pyproject.exists():
        return

    content = pyproject.read_text(encoding="utf-8")
    if 'requires-python = ">=3.14"' in content:
        patched = re.sub(
            r'requires-python\s*=\s*">=3\.14"',
            'requires-python = ">=3.12"',
            content,
            count=1,
        )
        pyproject.write_text(patched, encoding="utf-8")
        print("Patched pyproject.toml: >=3.14 -> >=3.12 for Colab.")


def install_project(repo_dir: str, relax_python_for_colab: bool = True) -> None:
    """Install project in editable mode."""
    if relax_python_for_colab:
        _relax_python_requirement_for_colab(repo_dir)

    _run("python -m pip install -U pip", cwd=repo_dir)
    _run("python -m pip install -e .", cwd=repo_dir)


def configure_imports(repo_dir: str) -> None:
    """Ensure Python can import project packages."""
    repo = str(Path(repo_dir).resolve())
    src = str((Path(repo_dir) / "src").resolve())
    if repo not in sys.path:
        sys.path.insert(0, repo)
    if src not in sys.path:
        sys.path.insert(0, src)


def run_pipeline(
    repo_dir: str,
    pdf_folder: str = "data/raw",
    workers: int = 1,
    confidence_threshold: float = 0.7,
    single: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Import and run the extraction pipeline."""
    configure_imports(repo_dir)
    os.chdir(repo_dir)

    from src.extraction.pipeline_runner import ExtractionPipeline  # noqa: WPS433

    pipeline = ExtractionPipeline(
        max_workers=workers,
        confidence_threshold=confidence_threshold,
    )

    if single:
        result = pipeline.process_document(Path(single))
        return [result]
    return pipeline.process_batch(pdf_folder=pdf_folder)


def setup_and_run(
    repo_url: str,
    repo_dir: str = "/content/document-refinery",
    pdf_folder: str = "data/raw",
    workers: int = 1,
    confidence_threshold: float = 0.7,
    single: Optional[str] = None,
    relax_python_for_colab: bool = True,
) -> list[dict[str, Any]]:
    """One-call helper: clone/pull, install, import, run."""
    clone_or_update(repo_url=repo_url, repo_dir=repo_dir)
    install_project(repo_dir=repo_dir, relax_python_for_colab=relax_python_for_colab)
    return run_pipeline(
        repo_dir=repo_dir,
        pdf_folder=pdf_folder,
        workers=workers,
        confidence_threshold=confidence_threshold,
        single=single,
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python colab_runner.py <repo_url> [pdf_folder]")

    _repo_url = sys.argv[1]
    _pdf_folder = sys.argv[2] if len(sys.argv) > 2 else "data/raw"
    outputs = setup_and_run(repo_url=_repo_url, pdf_folder=_pdf_folder)
    print(f"Completed: {len(outputs)} document(s)")

