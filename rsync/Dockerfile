FROM python:3.11-bullseye

# Set the working directory
WORKDIR /code/data-processing

# Install rsync
RUN apt-get update && apt-get install -y rsync && rm -rf /var/lib/apt/lists/*

# Default command to run the Python script
CMD ["python3", "-m", "rsync.rsync"]

# Docker commands to create image and run container:
# cd rsync
# docker build -t rsync .
# cd ..
# docker run -it --rm -v /data/dev/workspace/data-processing:/code/data-processing --name rsync rsync
# On Mac, run:
# docker run -it --rm -v ~/PycharmProjects/data-processing:/code/data-processing --name rsync rsync
