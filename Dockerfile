FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONPATH=/app:/app/newbot/exchangeMethods:/app/newbot/exchangeMethods/manage:/app/newbot/render:/app/newbot/client
CMD ["python", "-m", "portable.launch"]
