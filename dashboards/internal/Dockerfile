FROM python:3.12-slim

RUN apt-get update && apt-get install --no-install-recommends -y coinor-cbc\
   && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -m -s /bin/bash admin

RUN mkdir /app
RUN chown -R admin /app
USER admin

COPY . /app

WORKDIR /app

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health
EXPOSE 8501

CMD ["streamlit", "run", "Home.py", "--server.port=8501", "--server.address=0.0.0.0"]
