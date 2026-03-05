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

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional, Sequence


def _run(cmd: Sequence[str], cwd: Optional[str] = None) -> None:
    """Run a command and stream output."""
    print("$ " + " ".join(cmd))
    subprocess.run(cmd, cwd=cwd, check=True)


def _in_notebook() -> bool:
    return "ipykernel" in sys.modules


def clone_or_update(repo_url: str, repo_dir: str = "/content/document-refinery") -> str:
    """Clone repo if absent, otherwise pull latest."""
    repo_path = Path(repo_dir)
    parent = repo_path.parent
    parent.mkdir(parents=True, exist_ok=True)

    if (repo_path / ".git").exists():
        _run(["git", "pull", "--ff-only"], cwd=str(repo_path))
    elif repo_path.exists() and any(repo_path.iterdir()):
        raise ValueError(
            f"{repo_path} exists but is not a git repository. "
            "Choose a different repo_dir or remove this directory first."
        )
    else:
        _run(["git", "clone", repo_url, str(repo_path)])
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


def _ensure_docling(python_bin: str, repo_dir: str) -> None:
    """Ensure docling is importable in the active runtime."""
    try:
        import docling  # noqa: F401
        print("Docling is available.")
    except ImportError:
        print("Docling not found. Installing docling...")
        _run([python_bin, "-m", "pip", "install", "docling>=2.76.0"], cwd=repo_dir)


def install_project(
    repo_dir: str,
    relax_python_for_colab: bool = True,
    install_docling: bool = True,
) -> None:
    """Install project in editable mode."""
    if relax_python_for_colab:
        _relax_python_requirement_for_colab(repo_dir)

    python_bin = sys.executable or "python3"
    _run([python_bin, "-m", "pip", "install", "-U", "pip"], cwd=repo_dir)
    _run([python_bin, "-m", "pip", "install", "-e", "."], cwd=repo_dir)
    if install_docling:
        _ensure_docling(python_bin=python_bin, repo_dir=repo_dir)


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
    old_cwd = os.getcwd()
    try:
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
    finally:
        os.chdir(old_cwd)


def setup_and_run(
    repo_url: str,
    repo_dir: str = "/content/document-refinery",
    pdf_folder: str = "data/raw",
    workers: int = 1,
    confidence_threshold: float = 0.7,
    single: Optional[str] = None,
    relax_python_for_colab: bool = True,
    install_docling: bool = True,
) -> list[dict[str, Any]]:
    """One-call helper: clone/pull, install, import, run."""
    clone_or_update(repo_url=repo_url, repo_dir=repo_dir)
    install_project(
        repo_dir=repo_dir,
        relax_python_for_colab=relax_python_for_colab,
        install_docling=install_docling,
    )
    return run_pipeline(
        repo_dir=repo_dir,
        pdf_folder=pdf_folder,
        workers=workers,
        confidence_threshold=confidence_threshold,
        single=single,
    )


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clone/update, install, and run Document Refinery.",
    )
    parser.add_argument("repo_url", help="Git URL for the repository.")
    parser.add_argument("pdf_folder", nargs="?", default="data/raw")
    return parser.parse_args(argv)


if __name__ == "__main__":
    # In notebook/kernels, sys.argv usually contains "-f <kernel.json>".
    # Avoid treating Jupyter-injected args as CLI args.
    if _in_notebook():
        print(
            "Notebook context detected. Use:\n"
            "from colab_runner import setup_and_run\n"
            "results = setup_and_run(repo_url='https://github.com/<user>/document-refinery.git')"
        )
    else:
        args = _parse_args(sys.argv[1:])
        outputs = setup_and_run(repo_url=args.repo_url, pdf_folder=args.pdf_folder)
        print(f"Completed: {len(outputs)} document(s)")
