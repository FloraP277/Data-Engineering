FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
ENV PYTHONPATH=/app/src
RUN useradd --create-home app
USER app
# The run date defaults to today; the job reads DIRSYNC_DSN from a Kubernetes secret.
ENTRYPOINT ["python", "-m", "dirsync.run"]
