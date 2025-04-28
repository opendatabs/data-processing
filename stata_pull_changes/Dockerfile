FROM ubuntu:latest
RUN apt-get update && apt-get install -y git
WORKDIR /code
CMD ["/bin/bash", "/code/data-processing/stata_pull_changes/pull_changes.sh"]

# Commands to run the container with the published image locally:
# docker pull ghcr.io/opendatabs/data-processing/stata_pull_changes:latest
# docker run -it --rm -v /data/dev/workspace:/code --name stata_pull_changes ghcr.io/opendatabs/data-processing/stata_pull_changes:latest
