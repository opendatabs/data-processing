FROM python:3.10.2-bullseye
WORKDIR /code/data-processing
# ARG HTTPS_PROXY
# ENV HTTPS_PROXY=$HTTPS_PROXY
RUN python3 -m pip install --user --no-cache-dir pandas==2.0.2
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir requests_ntlm==1.2.0
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.0.10
CMD ["python3", "-m", "itbs_klv.etl"]

# Docker commands to create image and run container:
# cd itbs_klv
# docker build -t itbs_klv .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name itbs_klv itbs_klv
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name itbs_klv itbs_klv
