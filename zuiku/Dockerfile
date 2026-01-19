FROM python:3.10-slim

WORKDIR /app

# 复制依赖文件先安装，增强缓存
COPY requirements.txt .

# 使用清华源安装依赖
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple && \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY .. .

CMD ["python", "scheduler.py"]