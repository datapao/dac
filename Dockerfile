FROM python:3.7-slim

ADD . /app
WORKDIR /app

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install --trusted-host pypi.python.org -r requirements.txt

EXPOSE 5000

CMD ["python", "-m", "main", "ui"]
