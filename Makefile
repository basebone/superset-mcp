SHELL = /bin/bash

.PHONY: setup test test-one lock

setup:
	./setup.sh

# --- tests -------------------------------------------------------------------------

# 3.10 because that is what the deployed virtualenv runs, not the 3.13 in .python-version.
IMAGE ?= python:3.10-slim

# As the calling user, not as root. A container writing into the mounted checkout leaves
# what it builds -- *.egg-info, __pycache__, .pytest_cache -- owned by root, and a local
# `pip install -e .` then fails on it and cannot be cleaned up without sudo. Each recipe
# installs into a virtualenv inside the container, because a non-root user cannot write to
# the image's site-packages.
DOCKER_AS_ME = --user $(shell id -u):$(shell id -g)

test:
	docker run --rm $(DOCKER_AS_ME) -v "$(CURDIR):/w" -w /w -e HOME=/tmp $(IMAGE) bash -c \
		"python -m venv /tmp/v && /tmp/v/bin/pip install -q -r requirements.txt -r requirements-dev.txt && /tmp/v/bin/python -m pytest -q test_google_oauth.py test_token_store.py"

#   make test-one T=test_token_store.py
#   make test-one T='-k guest'
test-one:
	docker run --rm $(DOCKER_AS_ME) -v "$(CURDIR):/w" -w /w -e HOME=/tmp $(IMAGE) bash -c \
		"python -m venv /tmp/v && /tmp/v/bin/pip install -q -r requirements.txt -r requirements-dev.txt && /tmp/v/bin/python -m pytest -q $(T)"

# Regenerates requirements.txt from pyproject.toml's floors, resolved by the interpreter the
# deployed virtualenv runs, in a clean container every time -- so what comes out is what a
# fresh install gets and not what happens to be in a developer's virtualenv.
#
# --exclude-editable drops this project itself; setuptools is kept because the scanner reads
# it, and pip and wheel are filtered because they are not dependencies of anything.
lock:
	docker run --rm $(DOCKER_AS_ME) -v "$(CURDIR):/w" -w /w -e HOME=/tmp $(IMAGE) bash -c "python -m venv /tmp/v && /tmp/v/bin/pip install -q --upgrade pip setuptools wheel && /tmp/v/bin/pip install -q . && /tmp/v/bin/pip freeze --exclude-editable --all" 2>/dev/null | grep -E '^[A-Za-z0-9_.-]+==' | grep -viE '^(superset-mcp|pip|wheel)==' | sort -f > .lock.tmp
	sed -n '1,/^# --- pins below/p' requirements.txt > requirements.txt.new
	cat .lock.tmp >> requirements.txt.new
	mv requirements.txt.new requirements.txt
	rm -f .lock.tmp
	@echo "requirements.txt regenerated -- check the date in its header"
