FROM python:3.11-slim

WORKDIR /workspace

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY app/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app /workspace/app

ENV PYTHONPATH=/workspace
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
