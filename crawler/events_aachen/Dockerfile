FROM python:3.11-slim

COPY ./requirements.txt ./

RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && \
    apt-get install -y locales && \
    sed -i -e 's/# de_DE.UTF-8 UTF-8/de_DE.UTF-8 UTF-8/' /etc/locale.gen && \
    dpkg-reconfigure --frontend=noninteractive locales

ENV LANG de_DE.UTF-8
ENV LC_ALL de_DE.UTF-8

RUN useradd -m -s /bin/bash admin

RUN mkdir /src
RUN chown -R admin /src
USER admin

COPY ./main.py /src
COPY ./db_service.py /src

WORKDIR /src

CMD ["python", "-u", "./main.py"]
