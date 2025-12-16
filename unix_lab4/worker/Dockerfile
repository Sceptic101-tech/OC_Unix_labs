FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip3 install torch torchvision --index-url https://download.pytorch.org/whl/cpu
RUN pip3 install --no-cache-dir -r requirements.txt
CMD ["python", "worker.py"]
