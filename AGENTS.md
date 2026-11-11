# AGENTS.md

Guidance for coding agents working in this repository.

## What this is

A small Flask app ("Report Server") that pulls radiology and pathology reports
from hospital databases and serves them as HTML, plain text, JSON or RTF. It is
built for one specific site (GE Centricity RIS, Sectra CDWH, USB CDWH). It is
not a general solution, so don't generalize schemas or queries unless asked.

## Layout

- `web.py`: app factory `create_app()` and all routes (`/`, `/q`, `/cm`,
  `/acc2fall`, `/fall2acc`, `/acc`, `/sectra`, `/show`, `/distill`, `/download`).
- `src/repo/database/connection.py`: SQLAlchemy engines, one per source:
  - RIS: Oracle via `oracledb` (thick mode, needs the Oracle client installed)
  - Sectra CDWH: MS SQL via `pymssql`
  - USB CDWH: SAP HANA via `sqlalchemy-hana`
- `src/repo/database/*.py`: one module per query domain (`report`, `fall`,
  `termin`, `contrast_medium`, `sectra_cdwh`, `usb_cdwh`).
- `src/repo/report.py`, `parse.py`, `converter.py`, `writer.py`: RTF to text
  and HTML conversion and report parsing.
- `src/repo/distiller/`: rule-based extraction of measurements from report text
  (aorta, calcium, ventricle function), exposed via `/distill`.
- `templates/`, `static/`: Jinja templates and front-end JS. JS is bundled with
  Flask-Assets into `static/gen/packed.js`. Add new JS files to the bundle in
  `web.py`.
- `setup/*.sql`: legacy DB init and upgrade scripts. `scripts/`: one-off
  maintenance scripts.

## Routing rule worth knowing

`/sectra?accession_number=...` dispatches on the first character:
- starts with a digit: Sectra CDWH (radiology)
- anything else: USB CDWH pathology (`get_patho_report`)

`/show` and `/distill` only accept numeric accession numbers (RIS).

## Setup and running

- Python >= 3.12, managed with `uv` (`pyproject.toml` and `uv.lock` are the
  source of truth; `requirements.txt` is legacy).
- `uv sync` to install.
- Config: `src/repo/default_config.py`, overridden by `instance/config.cfg`.
  Credentials come from environment variables loaded from `.env` (`RIS_DB_*`,
  `SECTRA_*`, `CDWH_*`).
- Dev server: `./run-dev.sh` (Flask debug on port 7777).
- Production uses gunicorn with `"web:create_app()"`.

## Tests

- Tests use `unittest`. pytest is not installed.
- Run the working suite: `uv run python -m unittest tests.test_pathology -v`
- `uv run python -m unittest discover -s tests` currently reports import errors
  in `test_compare.py` (imports the removed `review` package) and the
  `test_distill_*` files (import `distiller` instead of `src.repo.distiller`).
  Don't treat these as regressions you caused. Fix them only if asked.
- Tests must never touch a real database. Mock the engine as
  `tests/test_pathology.py` does: `MagicMock()` engine, then assert on the SQL
  text and the bound parameters.

## Conventions

- Format with black. Keep the existing style of the module you edit.
- SQL: use `sqlalchemy.text()` with bound parameters (`:accession_number`).
  Never interpolate user input into SQL.
- Engines use `NullPool` where configured. Don't add connection pooling without
  checking with the maintainer.
- When behavior or endpoints change, update `README.md`. For releases, also
  bump `VERSION` in `web.py` and add an entry to `CHANGELOG.md`.

## Data safety (important)

This app handles real patient data.

- Never commit `.env`, `instance/`, `reports/`, `logs/` or any `*.csv` (all are
  gitignored; keep it that way).
- Don't open, print or copy the contents of local CSVs, `reports/` or logs into
  code, tests, commits or chat. Use synthetic values in tests (for example
  `TEST-001`).
- Don't log full report text or patient identifiers at INFO level or above.
- You can't reach the hospital databases from a dev machine in general. Don't
  try to connect to them. Ask the user to verify against real systems.

## Version control

- Main branch is `master`.
- The repo is also tracked with Jujutsu (`.jj/`). Prefer whatever the user is
  using and don't rewrite history without asking.
- Don't commit tool caches (`.pytest_cache/`, `.mypy_cache/`, `__pycache__/`).
