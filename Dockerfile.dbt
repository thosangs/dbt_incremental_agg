FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install uv
RUN pip install --no-cache-dir uv

# Copy requirements and install dependencies with uv
COPY requirements.txt ./
RUN uv pip install --system -r requirements.txt

COPY . .


