FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
CMD ["python3", "-m", "rsync.rsync"]

# Docker commands to create image and run container:
# cd stata_bik
# docker build -t rsync .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name rsync rsync
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name rsync rsync
