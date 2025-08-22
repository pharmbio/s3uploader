FROM python:3.10-slim

# Create and set the working directory in the container
WORKDIR /app

# Copy just requirements first (for caching optimization)
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the rest of the project files into the container
COPY . /app

# For example, if your main entrypoint is "main_uploader.py", run it by default:
CMD ["python", "main_uploader.py"]
