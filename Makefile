push:
	@echo "REMINDER: Set username = "__token__"
	@python -m twine upload dist/*

setup:
	@bash run_setup.sh

clean:
	@ruff check --fix .
	@pytest -rf tests/*