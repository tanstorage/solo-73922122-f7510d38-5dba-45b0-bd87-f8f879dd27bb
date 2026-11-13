FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY imapdemo /app/imapdemo

# 纯标准库实现，无第三方依赖
CMD ["python", "-m", "imapdemo"]
