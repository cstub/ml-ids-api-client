test:
	python -m pytest tests

lint:
	pylint ml_ids_api_client

lint-errors:
	pylint ml_ids_api_client -E

typecheck:
	mypy ml_ids_api_client