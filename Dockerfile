FROM python:3.13-slim
WORKDIR /app
COPY requirements-rebuild.txt requirements.txt
RUN pip install --root-user-action=ignore --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONPATH=/app
