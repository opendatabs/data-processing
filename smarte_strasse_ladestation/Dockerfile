# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]


# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/smarte_strasse_ladestation:latest
# docker run -it --rm --name smarte_strasse_ladestation ghcr.io/opendatabs/data-processing/smarte_strasse_ladestation:latest
