FROM python:3.11-bullseye
WORKDIR /code/data-processing
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0rc0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir requests_ntlm==1.2.0
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.2.0b1
RUN python3 -m pip install --user --no-cache-dir gssapi==1.8.3
RUN python3 -m pip install --user --no-cache-dir krb5==0.5.1
CMD ["python3", "-m", "itbs_klv.etl"]

# Docker commands to create image and run container:
# cd itbs_klv
# docker build -t itbs_klv .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name itbs_klv itbs_klv
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name itbs_klv itbs_klv
