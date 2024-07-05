FROM ubuntu:latest
ARG https_proxy
ENV https_proxy=${https_proxy}
RUN apt-get update && apt-get install -y git
WORKDIR /code
CMD ["/bin/bash", "/code/data-processing/stata_pull_changes/pull_changes.sh"]

# Docker commands to create image and run container:
# cd stata_pull_changes
# docker build --build-arg https_proxy="http://your-proxy-server:port" -t stata_pull_changes .
# cd ..
# docker run -it --rm -e https_proxy="http://your-proxy-server:port" -v /data/dev/workspace:/code --name stata_pull_changes stata_pull_changes
