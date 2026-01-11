-include .env
export

api\:fetch-spec:
	mkdir -p api
	curl -f "$(DIALOG_BASE_URL)/api/doc.json" > api/spec.json

api\:generate-client:
	openapi-python-client generate --path api/spec.json --output-path "api" --overwrite

api\:update:
	make api-fetch-spec
	make api-generate-client

app\:run:
	dialog run

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

