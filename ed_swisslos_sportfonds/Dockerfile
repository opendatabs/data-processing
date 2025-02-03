FROM python:3.12.3
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.32.3
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.2.0b1

CMD ["python3", "-m", "ed_swisslos_sportfonds.etl"]

# Docker commands to create image and run container:
# cd ed_swisslos_sportfonds
# docker build -t esc_faq .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name ed_swisslos_sportfonds ed_swisslos_sportfonds
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name ed_swisslos_sportfonds ed_swisslos_sportfonds
