FROM python:3.12.3
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.32.3
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir beautifulsoup4==4.12.3
RUN python3 -m pip install --user --no-cache-dir vobject==0.9.7

CMD ["python3", "-m", "ed_schulferien.etl"]

# Docker commands to create image and run container:
# cd ed_schulferien
# docker build -t ed_schulferien .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name ed_schulferien ed_schulferien
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name ed_schulferien ed_schulferien
