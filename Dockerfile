FROM python:3.14-slim

WORKDIR /app

COPY cdc_lakehouse ./cdc_lakehouse
COPY docs ./docs
COPY README.md ./

EXPOSE 8080

ENV PYTHONUNBUFFERED=1

CMD ["python3", "-m", "cdc_lakehouse.cli", "--base-dir", "/data", "serve"]

