FROM python:3.13.0-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY . .
RUN pip install -r requirements.txt

CMD ["python", "main.py"]
