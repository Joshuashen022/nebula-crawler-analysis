FROM python:3.12-slim

WORKDIR /app

# Install Python dependencies from requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and dist binary tree into the image
COPY ./src ./src
COPY ./dist ./dist
COPY .env .env
COPY ./database ./database
# Remove local results directory from the image if present
RUN rm -rf ./src/results

# Ensure nebula binary is executable if it exists
RUN if [ -d "./dist" ]; then find ./dist -type f -name "nebula" -exec chmod +x {} \;; fi

# Use src as working directory so relative paths in start.py work
WORKDIR /app/src

CMD ["python", "start.py"]

