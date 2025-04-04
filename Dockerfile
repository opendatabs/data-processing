# This Dockerfile is the base image for all data-processing etl jobs.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm

WORKDIR /code

# Bake in the common and ods_publish modules
COPY ./common /code/common
COPY ./ods_publish /code/ods_publish
