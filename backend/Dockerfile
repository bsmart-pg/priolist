# First stage: build

FROM python:3.11-slim-buster

COPY . /app

WORKDIR /app

RUN pip install --upgrade pip && pip install -r requirements.txt

EXPOSE 8081

CMD ["python", "app.py"]