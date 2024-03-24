python -m flake8 --extend-ignore=E203,W503 --max-line-length=120 klondike/ tests/
python -m black klondike/ tests/
python -m isort --profile black klondike/ tests/