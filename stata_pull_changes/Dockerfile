FROM ubuntu:latest
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
RUN apt-get update && apt-get install -y git
WORKDIR /code
CMD ["/bin/bash", "/code/data-processing/stata_pull_changes/pull_changes.sh"]

# Docker commands to create image and run container:
# cd mkb_sammlung_europa
# docker build --build-arg HTTPS_PROXY=https://your-proxy-server.com:pwd -t stadtreinigung_sauberkeitsindex
# cd ..
# docker run -it --rm -v /data/dev/workspace:/code --name stata_pull_changes stata_pull_changes
