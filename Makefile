compile:
	uv pip compile requirements.in -o requirements.txt --upgrade

install:
	uv pip install -r requirements.txt