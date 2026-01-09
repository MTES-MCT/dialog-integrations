api-fetch-spec:
	mkdir -p api
	curl -f "https://dialog.beta.gouv.fr/api/doc.json" > api/spec.json

api-generate-client:
	openapi-python-client generate --path api/spec.json --output-path "api" --overwrite

api-update:
	make api-fetch-spec
	make api-generate-client
