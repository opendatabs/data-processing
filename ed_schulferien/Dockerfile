# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

# Set environment variables for timezone and locale
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Zurich
ENV LANG=de_CH.UTF-8
ENV LANGUAGE=de_CH:de
ENV LC_ALL=de_CH.UTF-8

# Install required system packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    locales \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Configure the system locale
RUN echo "de_CH.UTF-8 UTF-8" >> /etc/locale.gen && \
    locale-gen de_CH.UTF-8 && \
    dpkg-reconfigure --frontend=noninteractive locales && \
    update-locale LANG=de_CH.UTF-8

# Set the timezone
RUN ln -fs /usr/share/zoneinfo/Europe/Zurich /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

CMD ["uv", "run", "-m", "etl"]

# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/ed_schulferien:latest
# docker run -it --rm -v /data/dev/workspace/data-processing/ed_schulferien/data:/code/data -v /data/dev/workspace/data-processing/ed_schulferien/data_orig:/code/data_orig -v /data/dev/workspace/data-processing/ed_schulferien/change_tracking:/code/change_tracking --name ed_schulferien ghcr.io/opendatabs/data-processing/ed_schulferien:latest
