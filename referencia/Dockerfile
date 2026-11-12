FROM python:3.12-slim
RUN pip install --no-cache-dir pyzmq==26.2.0 msgpack==1.1.0
WORKDIR /app
COPY referencia.py .
CMD ["python", "referencia.py"]
