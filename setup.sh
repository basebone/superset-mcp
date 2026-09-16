#!/bin/bash
# Creates the virtualenv this MCP server runs in and installs the project into it.
#
# Both the interpreter and the target directory are named rather than left to the
# environment: `python3` is 3.10 on the deploy host and 3.12 on a developer machine, and a
# virtualenv built by one cannot be used by the other.
#
#   ./setup.sh                                       # production defaults
#   PYTHON_BIN=python3.13 VENV_DIR=./.venv ./setup.sh
set -euo pipefail

MYDIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"

# 3.10 because that is what the virtualenv deployed on superset1 runs, and pyproject.toml
# requires only >= 3.10. Note .python-version says 3.13, which is what a developer using
# uv gets; the lock and this default follow the deployment.
#
# The absolute path, not the name: on log2 a hand-built /usr/local/bin/python3.10 sat
# ahead of the packaged one on PATH and had no sqlite3 module, which is not something to
# discover after the virtualenv has been deleted. This server keeps its OAuth tokens in a
# SQLite file too.
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3.10}"

# Flat, no /env, and named supersetmcp while the repository is superset-mcp. Both are what
# is deployed: the systemd unit in esme.etc names
# /smsc/var/venvs/supersetmcp/bin/python, so this path is not ours to tidy without
# changing that unit in the same round.
VENV_DIR="${VENV_DIR:-/smsc/var/venvs/supersetmcp}"

# Where the virtualenv goes, printed for a caller that needs to know without building it.
# Answered before anything is checked or created, so it is safe to ask anywhere.
if [ "${1:-}" = "--print-venv-dir" ]; then
    echo "$VENV_DIR"
    exit 0
fi

# The virtualenv belongs to root, the way the other four MCP servers' do. The up script
# no longer drops to sshsync with change_user -- it installs the systemd unit and restarts
# the service, which is root's work -- so every deploy arrives here as root and can remove
# what the last one built. That is the `rm: Permission denied` that stopped
# google-ads-mcp's rebuild, avoided by having one owner rather than by choosing sshsync.
#
# It also means the account the server runs as cannot rewrite the code it executes:
# sshsync only reads this tree.
#
# Checked before anything is deleted, and only when installing under /smsc/var/venvs: a
# developer naming VENV_DIR is not touching production and is left alone.
case "$VENV_DIR" in
    /smsc/var/venvs/*)
        if [ "$(id -un)" != "root" ]; then
            echo "run this as root, not $(id -un):" >&2
            echo "  sudo $0" >&2
            exit 1
        fi
        ;;
esac

# Group- and world-readable, so the sshsync the service runs as can read what root writes.
umask 022

cd "$MYDIR"

command -v "$PYTHON_BIN" >/dev/null || {
    echo "$PYTHON_BIN not found. Install it, or set PYTHON_BIN to an interpreter you have." >&2
    exit 1
}

# Checked before anything is deleted. `python -m venv` reports a missing ensurepip as a
# non-zero exit from ensurepip and nothing else, so without this the failure arrives after
# the rebuild below has removed the virtualenv that worked.
# Checked before anything is deleted, and checked at all because a hand-built interpreter
# without --enable-loadable-sqlite-extensions has no sqlite3 module, and MCP_TOKEN_DB
# points this server's OAuth tokens at a SQLite file. Without this the server installs
# cleanly and fails on the first login.
"$PYTHON_BIN" -c 'import sqlite3' 2>/dev/null || {
    echo "$PYTHON_BIN has no sqlite3 module, which the OAuth token database needs." >&2
    echo "On this host /usr/bin/python3.10 is the packaged one and has it." >&2
    exit 1
}

"$PYTHON_BIN" -c 'import ensurepip' 2>/dev/null || {
    echo "$PYTHON_BIN has no ensurepip, so it cannot create a virtualenv with pip in it." >&2
    echo "On Debian and Ubuntu that is the python3.x-venv package." >&2
    exit 1
}

# Removed and rebuilt, never installed into.
#
# `python -m venv` on a directory that already exists reuses it, and `pip install -r` does
# not uninstall what the file no longer lists, so every package a previous requirements.txt
# ever pulled stays in there. The security scanner reads what is installed rather than what
# is declared.
#
# VENV_KEEP=1 reuses a healthy one, for a developer iterating locally.
wanted_version="$("$PYTHON_BIN" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
venv_version="$("$VENV_DIR/bin/python" -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null || true)"

if [ -d "$VENV_DIR" ]; then
    if [ -n "${VENV_KEEP:-}" ] && [ "$venv_version" = "$wanted_version" ]; then
        echo "reusing the virtualenv at $VENV_DIR (VENV_KEEP is set)"
    else
        echo "removing the virtualenv at $VENV_DIR (python ${venv_version:-unusable}) and rebuilding it"
        rm -rf "$VENV_DIR"
    fi
fi

echo "creating $VENV_DIR with $("$PYTHON_BIN" --version)"
"$PYTHON_BIN" -m venv "$VENV_DIR"

# pip, setuptools and wheel first, and this is not housekeeping: this virtualenv carried a
# recent pip and the setuptools 59.6.0 the OS ships -- eight advisories up to 8.8 -- because
# it was created and never upgraded. The scanner reads every package installed in a
# virtualenv, not the ones the code imports.
"$VENV_DIR/bin/python" -m pip install --quiet --no-cache-dir --upgrade pip setuptools wheel

# The lock, and only the lock. requirements.txt is what decides the versions, and
# pyproject.toml's floors are what generated it.
#
# The project itself is deliberately not installed. The systemd unit runs
#   /smsc/var/venvs/supersetmcp/bin/python main.py --transport both
# with WorkingDirectory=/smsc/src/superset-mcp, so main, auth, token_store and google_oauth
# are imported from the checkout -- the deployed virtualenv has never held the package,
# only its dependencies. Installing it editable writes superset_mcp.egg-info into the
# checkout, and a checkout is not ours to write in: the tree already had a build/ and an
# egg-info owned by root from an install run as root in April, which made
# `pip install -e .` fail as sshsync with
#   error: Cannot update time stamp of directory 'superset_mcp.egg-info'
# Installing it non-editable would be worse: the code would be copied into site-packages
# and a deploy updating the checkout would no longer change what runs.
"$VENV_DIR/bin/python" -m pip install --quiet --no-cache-dir -r requirements.txt

# Verified by importing, not by running: the unit starts it with --transport both, so
# running it here would bind a port and wait. The import resolves every dependency the lock
# installed, which is what a bad lock breaks -- and google_oauth is named because it is the
# module whose dependencies used to be missing from the lock in the sibling project.
#
# From this directory, because that is how the unit imports them: nothing is installed.
( cd "$MYDIR" && "$VENV_DIR/bin/python" -c "import main, auth, token_store, google_oauth; import requests, google.auth; print('superset-mcp imports, google-auth and requests included')" )

echo
echo "done. the unit runs:  $VENV_DIR/bin/python main.py --transport both"
echo "python:   $VENV_DIR/bin/python"
echo "pip:      $("$VENV_DIR/bin/python" -m pip --version)"
