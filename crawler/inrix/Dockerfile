FROM python:3.11-slim

COPY ./requirements.txt ./

RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -m -s /bin/bash admin

RUN mkdir /src
RUN chown -R admin /src
USER admin

COPY . /src

WORKDIR /src

CMD ["python", "-u", "./main.py"]
