FROM python:3.13.3-bullseye
WORKDIR /code/data-processing
# ARG HTTPS_PROXY
# ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.3.5
RUN python3 -m pip install --user --no-cache-dir requests==2.26.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
RUN python3 -m pip install --user --no-cache-dir icalendar==4.0.9
CMD ["python3", "-m", "tba_abfuhrdaten.etl"]

# Docker commands to create image and run container:
# cd tba_abfuhrdaten
# docker build -t tba_abfuhrdaten .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name tba_abfuhrdaten tba_abfuhrdaten
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name tba_abfuhrdaten tba_abfuhrdaten
