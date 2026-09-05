FROM python:3.12.11-slim-bookworm
WORKDIR /app
COPY python/requirements.txt python/requirements-akshare.txt /app/python/
RUN python -m pip install --no-cache-dir -r /app/python/requirements-akshare.txt
COPY python /app/python
ENV PYTHONPATH=/app/python PYTHONUNBUFFERED=1
ENTRYPOINT ["python","-m","providers.akshare"]
