FROM python:3

WORKDIR /usr/src/meteoblue

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./etl.py" ]

# docker build -t meteoblue .
# docker run -it --rm --mount type=bind,source=$(pwd)/data,target=/data --name meteoblue meteoblue


