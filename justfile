port := "7777"

# list available recipes
default:
    @just --list

# install dependencies from uv.lock
sync:
    uv sync

# run the Flask dev server with reload
dev port=port:
    uv run flask --app "web:create_app()" --debug run --host 0.0.0.0 --port {{port}}

# run with gunicorn (production-style)
serve port=port workers="4":
    uv run gunicorn --bind 0.0.0.0:{{port}} --workers {{workers}} "web:create_app()"

# run the unit tests (default: the working pathology suite)
test target="tests.test_pathology":
    uv run python -m unittest {{target}} -v
