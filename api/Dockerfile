FROM python:3.10-slim
RUN useradd -ms /bin/bash admin

RUN pip install --upgrade pip

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get -y update; apt-get -y install curl

USER admin
WORKDIR /app

COPY . /app/api

ENTRYPOINT ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--timeout-keep-alive", "120", "--log-level", "critical"]