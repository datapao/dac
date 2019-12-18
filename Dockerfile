FROM python:3.7-slim

ARG config_path
ENV DAC_CONFIG_PATH=$config_path

ADD . /app
WORKDIR /app

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install --trusted-host pypi.python.org -r requirements.txt

RUN rm -f dac.db && python main.py create_db

EXPOSE 5000

RUN chmod +x scripts/dac.sh

CMD ["scripts/dac.sh"]

