FROM python:3.13-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy project files
COPY pyproject.toml uv.lock ./
COPY . .

# Install dependencies using uv (create virtual environment in project)
RUN uv sync --frozen

# Expose ports
EXPOSE 8000 8501

# Default command (will be overridden by docker-compose)
CMD ["uv", "run", "python", "main.py"]
