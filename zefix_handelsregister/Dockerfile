FROM python:3.11-bullseye
WORKDIR /code/data-processing
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0rc0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir SPARQLWrapper==2.0.0
RUN python3 -m pip install --user --no-cache-dir geopy==2.4.1
RUN python3 -m pip install --user --no-cache-dir rapidfuzz==3.6.1
CMD ["python3", "-m", "zefix_handelsregister.etl"]

# Docker commands to create image and run container:
# cd zefix_handelsregister
# docker build -t zefix_handelsregister .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name zefix_handelsregister zefix_handelsregister
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name zefix_handelsregister zefix_handelsregister
