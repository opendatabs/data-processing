FROM python:3.11-bullseye
WORKDIR /code/data-processing
RUN python3 -m pip install --user --no-cache-dir pandas==2.1.4
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir charset-normalizer==3.3.2
RUN python3 -m pip install --user --no-cache-dir icalendar==5.0.11
RUN python3 -m pip install --user --no-cache-dir rapidfuzz==3.6.1
CMD ["python3", "-m", "parlamentsdienst_gr_abstimmungen.etl"]

# Docker commands to create image and run container:
# cd parlamentsdienst_gr_abstimmungen
# docker build -t parlamentsdienst_gr_abstimmungen .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name parlamentsdienst_gr_abstimmungen parlamentsdienst_gr_abstimmungen
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name parlamentsdienst_gr_abstimmungen parlamentsdienst_gr_abstimmungen
