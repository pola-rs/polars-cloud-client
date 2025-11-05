# Configuration file for the Sphinx documentation builder.
# https://www.sphinx-doc.org/en/master/usage/configuration.html

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any

import sphinx_autosummary_accessors  # type: ignore[import-untyped]

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here.

# Add client directory
sys.path.insert(0, str(Path("../..").resolve()))


# -- Project information -----------------------------------------------------

project = "Polars Cloud"
author = "Polars"
copyright = f"2025, {author}"


# -- General configuration ---------------------------------------------------

extensions = [
    # Sphinx extensions
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.githubpages",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    # Third-party extensions
    "autodocsumm",
    "numpydoc",
    "sphinx_autosummary_accessors",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_favicon",
    "sphinx_llms_txt",
    "sphinx_reredirects",
    "sphinx_toolbox.more_autodoc.overloads",
]

# Render docstring text in `single backticks` as code.
default_role = "code"

maximum_signature_line_length = 88

# Below setting is used by
# sphinx-autosummary-accessors - build docs for namespace accessors like `Series.str`
# https://sphinx-autosummary-accessors.readthedocs.io/en/stable/
templates_path = ["_templates", sphinx_autosummary_accessors.templates_path]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["Thumbs.db", ".DS_Store"]

# Hide overload type signatures
# sphinx_toolbox - Box of handy tools for Sphinx
# https://sphinx-toolbox.readthedocs.io/en/latest/
overloads_location = ["bottom"]


# -- Extension settings ------------------------------------------------------

# sphinx.ext.intersphinx - link to other projects' documentation
# https://www.sphinx-doc.org/en/master/usage/extensions/intersphinx.html
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# numpydoc - parse numpy docstrings
# https://numpydoc.readthedocs.io/en/latest/
# Used in favor of sphinx.ext.
# napoleon for nicer render of docstring sections
numpydoc_show_class_members = False

# Sphinx-copybutton - add copy button to code blocks
# https://sphinx-copybutton.readthedocs.io/en/latest/index.html
# strip the '>>>' and '...' prompt/continuation prefixes.
copybutton_prompt_text = r">>> |\.\.\. "
copybutton_prompt_is_regexp = True

# redirect empty root to the actual landing page
redirects = {"index": "reference/index.html"}


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.
html_theme = "pydata_sphinx_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_css_files = ["css/custom.css"]  # relative to html_static_path
html_show_sourcelink = False

# key site root paths
static_assets_root = "https://raw.githubusercontent.com/pola-rs/polars-static/master"
web_root = "https://docs.pola.rs/polars-cloud"

# Specify version for version switcher dropdown menu
git_ref = os.environ.get("POLARS_CLOUD_VERSION", "main")
version_match = re.fullmatch(r"py-(\d+)\.\d+\.\d+.*", git_ref)
switcher_version = version_match.group(1) if version_match is not None else "dev"

html_js_files = [
    (
        "https://plausible.io/js/script.js",
        {"data-domain": "docs.cloud.pola.rs", "defer": "defer"},
    ),
]

html_theme_options = {
    "external_links": [
        {
            "name": "User guide",
            "url": f"{web_root}/",
        },
    ],
    "icon_links": [
        {
            "name": "Discord",
            "url": "https://discord.gg/4UfP5cfBE7",
            "icon": "fa-brands fa-discord",
        },
        {
            "name": "X/Twitter",
            "url": "https://x.com/datapolars",
            "icon": "fa-brands fa-x-twitter",
        },
        {
            "name": "Bluesky",
            "url": "https://bsky.app/profile/pola.rs",
            "icon": "fa-brands fa-bluesky",
        },
    ],
    "logo": {
        "image_light": f"{static_assets_root}/logos/polars-logo-dark-medium.png",
        "image_dark": f"{static_assets_root}/logos/polars-logo-dimmed-medium.png",
    },
    "show_version_warning_banner": False,
    "navbar_end": ["theme-switcher", "version-switcher", "navbar-icon-links"],
    "check_switcher": False,
}

# sphinx-favicon - Add support for custom favicons
# https://github.com/tcmetzger/sphinx-favicon
favicons = [
    {
        "rel": "icon",
        "sizes": "32x32",
        "href": f"{static_assets_root}/icons/favicon-32x32.png",
    },
    {
        "rel": "apple-touch-icon",
        "sizes": "180x180",
        "href": f"{static_assets_root}/icons/touchicon-180x180.png",
    },
]


def _minify_classpaths(s: str) -> str:
    # strip private polars classpaths, leaving the classname:
    # * "pl.Expr" -> "Expr"
    # * "polars.expr.expr.Expr" -> "Expr"
    # * "polars.lazyframe.frame.LazyFrame" -> "LazyFrame"
    # also:
    # * "datetime.date" => "date"
    s = s.replace("datetime.", "")
    return re.sub(
        pattern=r"""
        ~?
        (
          (?:pl|
            (?:polars\.
              (?:_reexport|datatypes)
            )
          )
          (?:\.[a-z.]+)?\.
          ([A-Z][\w.]+)
        )
        """,
        repl=r"\2",
        string=s,
        flags=re.VERBOSE,
    )


def process_signature(
    app: object,  # noqa: ARG001
    what: object,  # noqa: ARG001
    name: object,  # noqa: ARG001
    obj: object,  # noqa: ARG001
    opts: object,  # noqa: ARG001
    sig: str,
    ret: str,
) -> tuple[str, str]:
    return (
        _minify_classpaths(sig) if sig else sig,
        _minify_classpaths(ret) if ret else ret,
    )


def setup(app: Any) -> None:
    # TODO: a handful of methods do not seem to trigger the event for
    #  some reason (possibly @overloads?) - investigate further...
    app.connect("autodoc-process-signature", process_signature)
