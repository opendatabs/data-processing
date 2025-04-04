FROM python:3.12-slim
# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.6.8 /uv /uvx /bin/
WORKDIR /code/data-processing/jfs_gartenbaeder
# Copy uv files and install dependencies
COPY pyproject.toml .
COPY uv.lock .
RUN uv sync --frozen --no-cache


# PYTHONPATH needs to be the parent directory because of the common local module imports
ENV PYTHONPATH="/code/data-processing"

# Set environment variables for timezone and locale
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Zurich
ENV LANG=de_CH.UTF-8
ENV LANGUAGE=de_CH:de
ENV LC_ALL=de_CH.UTF-8

# Install required system packages 
# Need to run apt-get update first. Otherwise, the installation of locales will fail.
# Need to run rm -rf /var/lib/apt/lists/* to clean up the package cache and reduce the image size.
RUN apt-get update && apt-get install -y --no-install-recommends \
    locales=2.36-9+deb12u10 \
    tzdata=2025a-0+deb12u1 \
    && rm -rf /var/lib/apt/lists/*

# Configure the system locale
RUN echo "de_CH.UTF-8 UTF-8" >> /etc/locale.gen && \
    locale-gen de_CH.UTF-8 && \
    dpkg-reconfigure --frontend=noninteractive locales && \
    update-locale LANG=de_CH.UTF-8

# Set the timezone
RUN ln -fs /usr/share/zoneinfo/Europe/Zurich /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

CMD ["uv", "run", "-m", "src.etl"]


# Run the container with the published image:
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name jfs_gartenbaeder ghcr.io/opendatabs/data-processing/jfs_gartenbaeder:latest
