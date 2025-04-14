# This Dockerfile is planned to be the base image for all data-processing etl jobs.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm

# Set the working directory
WORKDIR /code
