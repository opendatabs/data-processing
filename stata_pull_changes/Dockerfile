FROM ubuntu:latest
RUN apt-get update && apt-get install -y git
WORKDIR /code
CMD ["/bin/bash", "/code/data-processing/stata_pull_changes/pull_changes.sh"]
