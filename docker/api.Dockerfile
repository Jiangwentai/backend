FROM python:3.12.11-slim-bookworm
WORKDIR /app
COPY python/requirements.txt /app/python/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/python/requirements.txt
COPY python /app/python
ENV PYTHONPATH=/app/python PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn","api.app:app","--host","0.0.0.0","--port","8000","--workers","1"]
