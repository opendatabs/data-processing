FROM python:3.7.6
WORKDIR /code 
RUN apt-get update
RUN apt-get install dialog apt-utils --yes
RUN apt-get install sudo --yes
RUN apt-get install postgresql --yes

# create local pip package from source: https://packaging.python.org/tutorials/packaging-projects/
RUN python3 -m pip install --user --upgrade setuptools wheel
RUN git clone https://github.com/opendatabs/vdv2pg.git
RUN cd /code/vdv2pg && pip install -r requirements.txt 
RUN cd /code/vdv2pg && python3 setup.py sdist bdist_wheel
RUN cd /code/vdv2pg && pip install dist/vdv2pg-0.0.2-py3-none-any.whl
COPY ./data/etl.sh .
# CMD ./etl.sh
CMD [ "/bin/bash", "./etl.sh"]

