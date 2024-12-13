FROM python:3.11-bullseye

# Set the working directory
WORKDIR /code/data-processing

# Install rsync
RUN apt-get update && apt-get install -y rsync && rm -rf /var/lib/apt/lists/*

# Set up SSH known_hosts dynamically for multiple servers
ARG SERVER_NAMES
RUN mkdir -p /root/.ssh && \
    echo "Adding servers to known_hosts: $SERVER_NAMES" && \
    for server in $(echo $SERVER_NAMES | tr ',' ' '); do \
        ssh-keyscan -H $server >> /root/.ssh/known_hosts; \
    done

# Default command to run the Python script
CMD ["python3", "-m", "rsync.rsync"]

# Docker commands to create image and run container:
# cd rsync
# docker build --build-arg SERVER_NAMES=server1,server2,server3 -t rsync .
# cd ..
# docker run -it --rm -v /home/syncuser/.ssh/id_rsa:/root/.ssh/id_rsa:ro -v /data/dev/workspace/data-processing:/code/data-proessing --name rsync rsync python3 -m rsync.rsync config_file.json