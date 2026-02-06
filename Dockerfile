FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.5.4-hadoop-3.3.6-v1

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source code
COPY src/ ./src/
COPY pyproject.toml .

# Install the project package in development mode
RUN pip install --no-cache-dir -e .

# Default command
CMD ["python3", "-m", "capstonellm.tasks.clean"]
