FROM bitnami/spark:latest

# Install necessary Python libraries
RUN pip install pandas matplotlib seaborn

# Set working directory
WORKDIR /app

# Copy dataset and script
COPY netflix_titles.csv /app/netflix_titles.csv
COPY eda_script.py /app/eda_script.py

# Run Spark job
CMD ["spark-submit", "/app/eda_script.py"]
