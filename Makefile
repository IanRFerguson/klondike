push:
	@echo "REMINDER: Set username = "__token__"
	@python -m twine upload dist/*

setup:
	@bash run_setup.sh


### CI Checks (these run locally in the CI pipeline, but can be run locally with `make all-checks`) ###

ruff:
	@uv run ruff check --fix .
	@uv run ruff format .

pytest:
	@uv run pytest -rf tests

mypy:
	@uv run mypy klondike

all-checks:
	@make ruff
	@make pytest
	@make mypy
