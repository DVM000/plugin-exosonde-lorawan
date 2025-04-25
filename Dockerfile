FROM python:3.11-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY ./app .

ENTRYPOINT ["python3", "main.py"]
