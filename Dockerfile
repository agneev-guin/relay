FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py mobile.html ./
# PORT is injected by Render (default 10000); fall back to 80 elsewhere
EXPOSE 10000
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-80}"]
