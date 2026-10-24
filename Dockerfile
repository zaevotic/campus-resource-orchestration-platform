FROM python:3.14-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY src/ ./src/
COPY schemas/ ./schemas/
COPY common/ ./common/
COPY static/ ./static/

# Expose the API port
EXPOSE 8000

# Start the FastAPI application
CMD ["uvicorn", "src.app:app", "--host", "0.0.0.0", "--port", "8000"]