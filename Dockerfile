FROM ghcr.io/astral-sh/uv:python3.14-alpine

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

COPY . .

RUN uv sync --locked

CMD [ "uv", "run", "json2mqtt.py" ]
