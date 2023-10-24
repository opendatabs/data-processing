FROM python:3.10.2-bullseye
WORKDIR /code/data-processing
# ARG HTTPS_PROXY
# ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==1.3.5
RUN python3 -m pip install --user --no-cache-dir requests==2.26.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.1.dev5
RUN python3 -m pip install --user --no-cache-dir SPARQLWrapper==2.0.0
CMD ["python3", "-m", "zefix_handelsregister.etl"]

# Docker commands to create image and run container:
# cd zefix_handelsregister
# docker build -t zefix_handelsregister .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name zefix_handelsregister zefix_handelsregister
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name zefix_handelsregister zefix_handelsregister
