FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
CMD ["python3", "-m", "staka_staatskalender.etl"]

# Docker commands to create image and run container:
# cd staka_staatskalender
# docker build -t staka_staatskalender .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name staka_staatskalender staka_staatskalender
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name staka_staatskalender staka_staatskalender
