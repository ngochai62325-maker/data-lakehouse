# Lakehouse Nhom 6

Project Delta Lakehouse using Spark, Airflow, and S3.

## Structure
- `etl_pipeline/`: Spark jobs for ingestion, bronze, silver, and gold layers.
- `dags/`: Airflow pipelines.
- `docker/`: Docker configurations.
- `dataset/`: Local datasets for Olist.

## How to Run the Project

### Prerequisites
- Docker and Docker Compose installed.

### 1. Build and Start Services
Run the following command to build the images and start all the services in detached mode:
```bash
docker compose up -d --build
```

### 2. Accessing the Services
Once the containers are up and running, you can access the following services via your browser:

- **Apache Airflow UI**: [http://localhost:8081](http://localhost:8081)
  - **Username**: `admin`
  - **Password**: `admin`
- **Spark Master UI**: [http://localhost:8080](http://localhost:8080)
- **Jupyter Notebook**: [http://localhost:8888](http://localhost:8888)
  - **Token**: `nhom6`
  - Use Jupyter for interactive data analysis or development.

### 3. Stopping the Services
To stop the services and remove the containers:
```bash
docker compose down
```
If you also want to clear out the database data (volumes):
```bash
docker compose down -v
```
