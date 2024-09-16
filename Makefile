push:
	@echo "REMINDER: Set username = __token__"
	@python -m twine upload dist/*

setup:
	@bash run_setup.sh

test:
	@cd tests && pytest -rf -W ignore::DeprecationWarning .