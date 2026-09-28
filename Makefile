SHELL = /bin/bash

.PHONY: setup reinstall_mcp_auth test test-one lock

setup:
	./setup.sh

# --- esme_mcp ----------------------------------------------------------------------

# esme_mcp, the OAuth authorization server and HTTP layer every remote MCP here runs, is
# on no index, so it comes from its repository at an exact tag. Pinned and not a floor: a
# deploy that resolves whatever master happens to be is a deploy nobody can reproduce.
ESME_MCP_VERSION ?= 1.4.1
ESME_MCP_URL = git+https://svn2:2b8ca4d28b1fec83c6e8d4162998c8a12621b26e@github.com/basebone/esme.mcp-py.git@$(ESME_MCP_VERSION)

# Called by setup.sh with PIP pointing at the virtualenv it just built. With its
# dependencies: esme_mcp declares google-auth, requests and cryptography, and this server
# no longer does. The lock is reapplied afterwards so its pins win over whatever the
# resolver picked.
reinstall_mcp_auth:
	$(PIP) uninstall --no-cache-dir esme_mcp -y
	$(PIP) install --no-cache-dir $(ESME_MCP_URL)
	$(PIP) install --no-cache-dir -r requirements.txt

# Where esme.mcp-py is checked out. Override on the command line if it is not next to this
# repository: make test ESME_MCP=/somewhere/else
#
# From the checkout and not from the pinned tag, because python:3.12-slim carries no git
# and a pip that cannot clone cannot install a git+https URL. What the deploy installs is
# the tag; setup.sh is where that is verified.
ESME_MCP ?= ../esme.mcp-py

# Writable, not :ro: setuptools touches src/esme_mcp.egg-info while working out the
# requirements, so a read-only mount fails with "Cannot update time stamp of directory".
MOUNT_ESME_MCP = -v "$(abspath $(ESME_MCP)):/esmemcp"

# --- tests -------------------------------------------------------------------------

# 3.12 because that is what the deployed virtualenv runs, not the 3.13 in .python-version.
# pyproject.toml requires >= 3.11 for esme_mcp; on 3.10 pip refuses the project outright.
IMAGE ?= python:3.12-slim

# As the calling user, not as root. A container writing into the mounted checkout leaves
# what it builds -- *.egg-info, __pycache__, .pytest_cache -- owned by root, and a local
# `pip install -e .` then fails on it and cannot be cleaned up without sudo. Each recipe
# installs into a virtualenv inside the container, because a non-root user cannot write to
# the image's site-packages.
DOCKER_AS_ME = --user $(shell id -u):$(shell id -g)

test:
	docker run --rm $(DOCKER_AS_ME) -v "$(CURDIR):/w" $(MOUNT_ESME_MCP) -w /w -e HOME=/tmp $(IMAGE) bash -c \
		"python -m venv /tmp/v && /tmp/v/bin/pip install -q -r requirements.txt -r requirements-dev.txt && /tmp/v/bin/pip install -q /esmemcp && /tmp/v/bin/pip install -q -r requirements.txt && /tmp/v/bin/python -m pytest -q test_http_config_env.py test_audited_tool.py"

#   make test-one T=test_token_store.py
#   make test-one T='-k guest'
test-one:
	docker run --rm $(DOCKER_AS_ME) -v "$(CURDIR):/w" $(MOUNT_ESME_MCP) -w /w -e HOME=/tmp $(IMAGE) bash -c \
		"python -m venv /tmp/v && /tmp/v/bin/pip install -q -r requirements.txt -r requirements-dev.txt && /tmp/v/bin/pip install -q /esmemcp && /tmp/v/bin/pip install -q -r requirements.txt && /tmp/v/bin/python -m pytest -q $(T)"

# Regenerates requirements.txt from pyproject.toml's floors, resolved by the interpreter the
# deployed virtualenv runs, in a clean container every time -- so what comes out is what a
# fresh install gets and not what happens to be in a developer's virtualenv.
#
# esme_mcp is installed from the checkout beside it so its dependencies are locked too, and
# then filtered out: setup.sh installs it from the pinned tag.
#
# --exclude-editable drops this project itself; setuptools is kept because the scanner reads
# it, and pip and wheel are filtered because they are not dependencies of anything.
lock:
	docker run --rm $(DOCKER_AS_ME) -v "$(CURDIR):/w" $(MOUNT_ESME_MCP) -w /w -e HOME=/tmp $(IMAGE) bash -c "python -m venv /tmp/v && /tmp/v/bin/pip install -q --upgrade pip setuptools wheel && /tmp/v/bin/pip install -q . /esmemcp && /tmp/v/bin/pip freeze --exclude-editable --all" 2>/dev/null | grep -E '^[A-Za-z0-9_.-]+==' | grep -viE '^(superset-mcp|esme-mcp|esme_mcp|pip|wheel)==' | sort -f > .lock.tmp
	sed -n '1,/^# --- pins below/p' requirements.txt > requirements.txt.new
	cat .lock.tmp >> requirements.txt.new
	mv requirements.txt.new requirements.txt
	rm -f .lock.tmp
	@echo "requirements.txt regenerated -- check the date in its header"
