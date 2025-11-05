# Minimal makefile for Sphinx documentation

SHELL=bash
VENV=.venv

ifeq ($(OS),Windows_NT)     # is Windows_NT on XP, 2000, 7, Vista, 10...
    VENV_FOLDER := Scripts
else
    VENV_FOLDER := bin
endif
VENV_BIN=../../$(VENV)/$(VENV_FOLDER)

export BUILDING_SPHINX_DOCS = 1

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?= -j auto -W
SPHINXBUILD   ?= $(VENV_BIN)/sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

clean:
	@rm -rf source/reference/*/api/
	@rm -rf source/reference/api/
	@rm -rf "$(BUILDDIR)"

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
