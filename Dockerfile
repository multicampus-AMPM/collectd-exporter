FROM python:3.9-slim
WORKDIR /home
COPY . .
RUN pip install --no-cache-dir --requirement requirements.txt
CMD ["python", "./exporter.py"]
