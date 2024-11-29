FROM python:3.12.3
WORKDIR /code/data-processing
#ARG HTTPS_PROXY
#ENV HTTPS_PROXY=$HTTPS_PROXY
COPY requirements.txt requirements.txt
COPY .ods_utils_py.env .ods_utils_py.env
RUN python3 -m pip install --user --no-cache-dir more-itertools==10.2.0
RUN python3 -m pip install --user --no-cache-dir -r requirements.txt

CMD ["python3", "-m", "stata_ods.daily_jobs.update_temporal_coverage.etl"]

# Docker commands to create image and run container:
# cd stata_ods/daily_jobs/update_temporal_coverage
# docker build -t update_temporal_coverage .
# cd ../../..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name update_temporal_coverage update_temporal_coverage
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name update_temporal_coverage update_temporal_coverage
