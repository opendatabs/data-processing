FROM ghcr.io/astral-sh/uv:python3.12-bookworm

WORKDIR /code

COPY ./common /code/common
COPY ./ods_publish /code/ods_publish
