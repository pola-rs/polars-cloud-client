r"""Build previous `polars-cloud` client docs versions for a `gh-pages` branch backfill.

Each ref's HTML docs lands in `WORKDIR/api/python/version/<major>.<minor>/`; merge this
content into a checkout of `gh-pages`, commit and push. A run of the
`release-client-documentation.yaml` workflow will regenerate the `version_switcher.json`
and `sitemap_index.xml` files.
"""

import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

BASE_URL = "https://docs.cloud.pola.rs"

REPODIR = pathlib.Path(__file__).resolve().parents[2]
WORKDIR = REPODIR / "_client-docs-backfill"


def build_ref_docs(ref: str) -> None:
    """Build the docs of a given git ref.

    For the given git ref (an exact release tag, that is, "client-0.8.0"):

    1. check out a worktree,
    2. overlay the current `conf.py` and `requirements-docs.txt` onto it (to include all
       recent extensions),
    3. build a scratch virtual environment with `uv`,
    4. install that release from `polars-cloud` and the Sphinx dependencies,
    5. run `make html` with `POLARS_CLOUD_DOCS_BASE_URL` set to its future URL
       destination to enable sitemap generation,
    6. delete the virtual environment and remove the git worktree.

    Any ref that fails to build is simply skipped.
    """
    worktree = WORKDIR / "worktrees" / ref
    docsdir = worktree / "client" / "docs"

    if (m := re.fullmatch(r"client-(\d+\.\d+\.\d+).*", ref)) is None:
        msg = f"Not a release tag: {ref!r}"
        raise ValueError(msg)
    version = m.group(1)

    try:
        if not worktree.exists():
            subprocess.run(
                ["git", "worktree", "add", str(worktree), ref],
                check=True,
                cwd=REPODIR,
            )

        for p in ("source/conf.py", "requirements-docs.txt"):
            shutil.copy2(REPODIR / "client" / "docs" / p, docsdir / p)

        with tempfile.TemporaryDirectory() as venv:
            _uv_pip_install = [
                "uv",
                "pip",
                "install",
                "--link-mode",
                "copy",
                "--python",
                f"{venv}/bin/python",
            ]

            subprocess.run(
                ["uv", "venv", venv],
                check=True,
            )
            subprocess.run(
                [*_uv_pip_install, f"polars-cloud=={ref.removeprefix('client-')}"],
                check=True,
            )
            subprocess.run(
                [*_uv_pip_install, "-r", str(docsdir / "requirements-docs.txt")],
                check=True,
            )
            subprocess.run(
                ["make", "html"],
                check=True,
                cwd=docsdir,
                env={
                    **os.environ,
                    "BUILDING_SPHINX_DOCS": "1",
                    "POLARS_CLOUD_DOCS_BASE_URL": f"{BASE_URL}/api/python/version/{version}/",
                    "POLARS_CLOUD_VERSION": version,
                    "SPHINXBUILD": f"{venv}/bin/sphinx-build",
                },
            )

        target = WORKDIR / "api" / "python" / "version" / version
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(docsdir / "build" / "html", target)
    except Exception as exc:
        sys.stderr.write(f"\n\nERROR: Skipping {ref}: {exc}\n\n")
    finally:
        subprocess.run(
            ["git", "worktree", "remove", "--force", str(worktree)],
            check=False,
            cwd=REPODIR,
        )


if __name__ == "__main__":
    for ref in sys.argv[1:]:
        build_ref_docs(ref)
    shutil.rmtree(WORKDIR / "worktrees", ignore_errors=True)
    subprocess.run(
        ["git", "worktree", "prune"],
        check=False,
        cwd=REPODIR,
    )
