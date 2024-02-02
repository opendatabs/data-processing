FROM python:3.11-bullseye
WORKDIR /code/data-processing
RUN python3 -m pip install --user --no-cache-dir pandas==2.2.0
RUN python3 -m pip install --user --no-cache-dir requests==2.31.0
RUN python3 -m pip install --user --no-cache-dir filehash==0.2.dev1
RUN python3 -m pip install --user --no-cache-dir openpyxl==3.2.0b1
CMD ["python3", "-m", "staka_kandidaturen.etl"]

# Docker commands to create image and run container:
# cd staka_kandidaturen
# docker build -t staka_kandidaturen .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name staka_kandidaturen staka_kandidaturen

# Docker on Mac:
# docker run -it --rm -v /path/to/data-processing:/code/data-processing --name staka_kandidaturen staka_kandidaturen
