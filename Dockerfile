# Use a slim Python 3.11 image to keep the satellite edge node lightweight
FROM python:3.11-alpine

# Set the working directory inside the container
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "run_edge_flow.py"]