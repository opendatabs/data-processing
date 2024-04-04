FROM ubuntu:latest
RUN apt-get update && apt-get install -y git
WORKDIR /code
CMD ["/bin/bash", "/code/data-processing/stata_pull_changes/pull_changes.sh"]

# Docker commands to create image and run container:
# cd mkb_sammlung_europa
# docker build -t stata_pull_changes .
# cd ..
# docker run -it --rm -v /data/dev/workspace:/code --name stata_pull_changes stata_pull_changes