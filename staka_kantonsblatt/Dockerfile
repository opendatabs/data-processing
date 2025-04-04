FROM python:3.11-bullseye
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir python-dotenv==1.1.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.2.0b1
CMD ["python3", "-m", "staka_kantonsblatt.etl"]

# Docker commands to create image and run container:
# cd staka_kantonsblatt
# docker build -t staka_kantonsblatt .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name staka_kantonsblatt staka_kantonsblatt
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name staka_kantonsblatt staka_kantonsblatt