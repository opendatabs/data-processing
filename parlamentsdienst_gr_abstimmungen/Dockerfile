FROM python:3.10.2-bullseye
WORKDIR /code/data-processing
# ARG HTTPS_PROXY
# ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.3.5
RUN python3 -m pip install --user --no-cache-dir requests==2.26.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
RUN python3 -m pip install --user --no-cache-dir charset-normalizer==2.0.12
RUN python3 -m pip install --user --no-cache-dir icalendar==4.0.9
CMD ["python3", "-m", "parlamentsdienst_gr_abstimmungen.etl"]

# Docker commands to create image and run container:
# cd parlamentsdienst_gr_abstimmungen
# docker build -t parlamentsdienst_gr_abstimmungen .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name parlamentsdienst_gr_abstimmungen parlamentsdienst_gr_abstimmungen
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name parlamentsdienst_gr_abstimmungen parlamentsdienst_gr_abstimmungen
