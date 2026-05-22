# Security Policy

## Supported Versions

Security fixes are released on the latest `0.x` line of `stateforward.hsm`.
Consumers should run the newest published patch version before reporting an
issue that may already be fixed.

## Reporting A Vulnerability

Do not open a public issue for a suspected vulnerability. Report it privately
through GitHub's private vulnerability reporting for:

https://github.com/stateforward/hsm.py/security/advisories/new

If that channel is unavailable, contact a repository maintainer directly and
include enough detail to reproduce the issue.

Useful reports include:

- affected `stateforward.hsm` version
- Python version and operating system
- a minimal model or event trace that reproduces the behavior
- whether the issue requires untrusted model definitions, untrusted event data,
  or only normal application usage
- expected and observed behavior

## Security Model

`stateforward.hsm` is a local Python library. It does not open network sockets,
spawn subprocesses, deserialize arbitrary bytes, or execute model definitions
from strings. User-provided callbacks are application code and execute with the
same privileges as the host process.

Applications should not treat untrusted model definitions or callbacks as a
sandbox boundary. Validate untrusted event payloads and operation arguments at
application boundaries before dispatching them into a state machine.

## Release Verification

Before a security or production release, maintainers should run the same gates
that CI enforces:

```bash
uv sync --group dev
uv export --quiet --all-groups --no-emit-project --format requirements.txt --output-file audit-requirements.txt
uv run pip-audit -r audit-requirements.txt --require-hashes --disable-pip --strict --progress-spinner off
uv run pytest -vv -W error --cov=hsm --cov-report=term-missing --cov-fail-under=90
uv run pyright
uv build
uvx twine check dist/*
```

For changes that affect timers, cancellation, dispatch ordering, lifecycle, or
resource cleanup, also run:

```bash
HSM_SOAK=1 uv run pytest tests/test_soak.py -q -W error
```
