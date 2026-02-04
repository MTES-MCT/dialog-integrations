-include .env
export

api\:fetch-spec:
	mkdir -p api
	curl -f "$(DIALOG_BASE_URL)/api/doc.json" > api/spec.json

api\:generate-client:
	uv run openapi-python-client generate --path api/spec.json --output-path "api" --overwrite

api\:update:
	make api:fetch-spec
	make api:generate-client

app\:build-ci:
	uv python install 3.11
	uv sync --frozen

app\:build:
	uv sync

app\:lint:
	uv run ruff check .

app\:format:
	uv run ruff format .

app\:format-check:
	uv run ruff format --check .

app\:type:
	uv run pyright

app\:prepare-commit:
	uv run ruff format .
	uv run ruff check . --fix
	uv run pyright

app\:test:
	uv run pytest -v

app\:test-watch:
	uv run pytest -v --watch
