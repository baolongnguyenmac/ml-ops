compile:
	uv pip compile requirements.in -o requirements.txt

install:
	uv pip install -r requirements.txt