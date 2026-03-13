FROM python:3.12-slim

WORKDIR /app

# Install minimal runtime dependencies if needed later
# RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and dist binary tree into the image
COPY ./src ./src
COPY ./dist ./dist
# Remove local results directory from the image if present
RUN rm -rf ./src/results

# Ensure nebula binary is executable if it exists
RUN if [ -d "./dist" ]; then find ./dist -type f -name "nebula" -exec chmod +x {} \;; fi

# Use src as working directory so relative paths in start.py work
WORKDIR /app/src

CMD ["python", "start.py"]

