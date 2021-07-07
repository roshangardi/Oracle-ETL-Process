#Airflow Oracle ETL Process

### Airflow ETL Application:
ETL Process defined as Airflow job, running on docker container, usign a python DAG file to load data into Oracle Server.

### Application Setup:
- Install Docker on the host system.
- Install Docker Compose v1.27.0 or higher
- Initialize airflow using:
  `$ docker-compose up airflow-init`
- Start services using docker-compose:
  `$ docker-compose up`
- Access the web interface at http://localhost:8080
-  The default account has the login airflow and the password airflow.

### Clean Up:
- Stop and delete docker containers, delete volumes with database data and download images, run:
`$ docker-compose down --volumes --rmi all`


### Airflow Pipeline:

![AirflowExample](https://github.com/roshangardi/Oracle-ETL-Process/blob/main/Images/AirflowETL.PNG?raw=true)


### Application Stack:
- Python.
- Docker.
- Airflow
