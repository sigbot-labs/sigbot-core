ARG PYTHON_IMAGE=docker.io/library/python:3.12-slim
FROM ${PYTHON_IMAGE}
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common /app/common
COPY config /app/config
COPY deploy/mock-truthsocial-service/app /app/service
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 65532:65532
EXPOSE 8080
CMD ["uvicorn", "service.main:app", "--host", "0.0.0.0", "--port", "8080"]
