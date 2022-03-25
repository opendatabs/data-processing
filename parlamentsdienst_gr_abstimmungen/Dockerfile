FROM python:3.10.0
WORKDIR /code/data-processing
ARG HTTPS_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.3.5
RUN python3 -m pip install --user --no-cache-dir requests==2.27.1
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
RUN python3 -m pip install --user --no-cache-dir ics==0.7
CMD ["python3", "-m", "parlamentsdienst_gr_abstimmungen.etl"]

# Docker commands to create image and run container:
# cd parlamentsdienst_gr_abstimmungen
# docker build -t parlamentsdienst_gr_abstimmungen .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name parlamentsdienst_gr_abstimmungen parlamentsdienst_gr_abstimmungen
