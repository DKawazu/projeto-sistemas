FROM python:3.12-slim
RUN pip install --no-cache-dir pyzmq==26.2.0
WORKDIR /app
COPY broker.py .
CMD ["python", "broker.py"]
