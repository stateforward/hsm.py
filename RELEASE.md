# Release Checklist

Use this checklist for any PyPI release of `stateforward.hsm`.

## Preflight

- Confirm the working tree contains only intentional changes.
- Confirm `pyproject.toml`, `hsm/version.py`, and `uv.lock` agree on the
  release version.
- Keep unrelated generated outputs, local benchmark scripts, and dirty sibling
  submodules out of the release commit.

## Required Gates

```bash
uv sync --group dev
uv export --quiet --all-groups --no-emit-project --format requirements.txt --output-file audit-requirements.txt
uv run pip-audit -r audit-requirements.txt --require-hashes --disable-pip --strict --progress-spinner off
uv run pytest -q -W error --cov=hsm --cov-report=term-missing --cov-fail-under=90
uv run pyright
HSM_SOAK=1 uv run pytest tests/test_soak.py -q -W error
uv build
uvx twine check dist/stateforward_hsm-<version>*
```

Confirm the wheel and source distribution contain the typed marker and release
docs before upload:

```bash
python3 - <<'PY'
import glob
import tarfile
import zipfile

[wheel] = glob.glob("dist/*.whl")
with zipfile.ZipFile(wheel) as archive:
    wheel_names = set(archive.namelist())
assert "hsm/py.typed" in wheel_names

[sdist] = glob.glob("dist/*.tar.gz")
with tarfile.open(sdist) as archive:
    sdist_names = set(archive.getnames())
root = next(name for name in sdist_names if name.endswith("/PKG-INFO")).rsplit("/", 1)[0]
required = {
    f"{root}/README.md",
    f"{root}/RELEASE.md",
    f"{root}/SECURITY.md",
    f"{root}/hsm/py.typed",
    f"{root}/pyproject.toml",
}
missing = required - sdist_names
assert not missing, sorted(missing)
PY
```

Run a local wheel smoke test before upload:

```bash
tmpdir=$(mktemp -d)
python3 -m venv "$tmpdir/venv"
"$tmpdir/venv/bin/python" -m pip install dist/stateforward_hsm-<version>-py3-none-any.whl
"$tmpdir/venv/bin/python" - <<'PY'
import asyncio
import hsm

class Instance(hsm.Instance):
    pass

async def main():
    model = hsm.Define(
        "Smoke",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle", hsm.Transition(hsm.On("go"), hsm.Target("../done"))),
        hsm.State("done"),
    )
    instance = Instance()
    await hsm.Started(None, instance, model)
    await hsm.Dispatch(None, instance, hsm.Event(name="go"))
    assert instance.state() == "/Smoke/done"
    assert hsm.TakeSnapshot(None, instance).QueueLen == 0
    await hsm.Stop(instance)

asyncio.run(main())
PY
rm -rf "$tmpdir"
```

## Publish

```bash
set -a
source ../../hsm.py/.env
set +a
test -n "$PYPI_TOKEN"
uvx twine upload --username __token__ --password "$PYPI_TOKEN" dist/stateforward_hsm-<version>*
```

After upload, verify the package from PyPI with `--no-cache-dir`. The PyPI
simple index can lag briefly after upload; retry after a short delay if the new
version is visible on the project page but not yet installable.

```bash
tmpdir=$(mktemp -d)
python3 -m venv "$tmpdir/venv"
"$tmpdir/venv/bin/python" -m pip install --no-cache-dir --index-url https://pypi.org/simple stateforward.hsm==<version>
"$tmpdir/venv/bin/python" - <<'PY'
import hsm
print(hsm.__version__)
PY
rm -rf "$tmpdir"
```

## Push And Monitor

- Push the Python package commit to `stateforward/hsm.py`.
- Update and push the monorepo submodule pointer.
- Confirm GitHub Actions Python CI is green across Ubuntu, macOS, and Windows.
