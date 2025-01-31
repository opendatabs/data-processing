FROM python:3.12.3
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.32.3
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir markdown==3.7
RUN python3 -m pip install --user --no-cache-dir markdown-newtab==0.2.0

CMD ["python3", "-m", "esc_faq.etl"]

# Docker commands to create image and run container:
# cd esc_faq
# docker build -t esc_faq .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name esc_faq esc_faq
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name esc_faq esc_faq
