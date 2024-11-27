### CDE-CAPSTONE

Background Story

A travel Agency reached out to CDE, their business model involves recommending tourist location to their customers based on different data points, they want one of our graduates to build a Data Platform that will process the data from the Country rest API HERE into their cloud based Database/Data Warehouse for predictive analytics by their Data Science team.


### Overview
The goal of this project is to design and implement a scalable data platform that:

-   Extracts country data from a REST API.
-   Stores raw data in a Cloud Object Storage (S3) in Parquet format.
-   Transforms and loads specific attributes into a cloud-based database (Amazon RDS).
-   Models data into fact and dimension tables using DBT for analytics readiness.
-   Enables predictive analytics for the agency’s data science team.

### Data Architecture
![data_work_flow](/images/capstone_data_architecture.png)

#### Key Steps
-   Data Extraction: Pulling data from the REST API.
-   Data Storage: Storing raw API data in S3 (Parquet format).
-   Data Transformation: Extracting required fields and writing to the Amazon RDS.
-   Modeling: Using DBT to structure data into fact and dimension tables.
-   Orchestration: Managed using Apache Airflow.

#### Features
-   Automated data pipeline orchestration using Airflow.
-   Scalable storage of raw data in Parquet format for future flexibility.
-   Data modeling for analytics readiness with DBT.
-   Robust CI/CD integration for automated testing and deployments.
-   Fully containerized extraction process for reproducibility.
-   Infrastructure automation with Terraform for consistent and repeatable setups.

### Choice of Tools
#### Data processing, Cloud and Storage
- AWS wrangler & boto3:
    - AWS SDK for Python
    - Capable of managing interactions with AWS services (S3)
    - Easy file uploads/downloads to/from S3
    - Capacity to manage AWS authentication and session handling.

- AWS S3 (Simple Storage Service):

    - Acts as the data lake for raw data storage.
    - Chosen for its scalability, cost-effectiveness, and ability to store data in Parquet format, which is optimized for performance and analytics.

- Amazon RDS (Relational Database Service):

    - Used as the database for storing transformed data, making it accessible to the Data Science team for predictive analytics.
    - Provides managed services, reducing the operational overhead of database management.
    - Compatible with DBT for SQL-based modeling.
#### Orchestration
- Apache Airflow:
    - Allows workflow orchestration with clear task dependencies (Extract, Transform, and Load).
    - Handles scheduling and retries, ensuring a reliable pipeline.
    - Open-source and widely used in data engineering pipelines for its flexibility.

#### Data Transformation and Modeling
- DBT (Data Build Tool):
    - Simplifies SQL-based transformations to create fact and dimension tables.
    - Ensures data models are reusable, testable, and version-controlled.
    - Compatible with Amazon RDS, making it ideal for analytics-ready data.

#### Containerization
- Docker:
    - Provides a consistent and isolated environment for running extraction and transformation scripts.
    - Facilitates CI/CD by bundling code into an image for deployment to any environment.
    - Makes the process portable and reduces setup friction.

#### Infrastructure as Code
- Terraform:
Automates the provisioning of cloud resources (S3 buckets, RDS, IAM roles).
Ensures consistency and version control for infrastructure.
Manages Terraform state in a cloud-based backend for collaboration and scalability.

#### CI/CD
- GitHub Actions:
    - Automates code linting, testing, and Docker image builds.
    Pushes the Docker image to a Cloud Container Registry for deployment.
    - Reduces manual deployment steps and ensures code quality.

#### Version Control
- Git/GitHub:
    - Used to manage code repositories and integrate CI/CD pipelines.
    - Ensures collaboration and versioning.


### Setup and Installation

#### Prerequisites
- AWS account with permissions to create resources (S3, RDS).
- Docker installed on your machine.
- Python 3.x installed.
- Terraform installed.
- AWS cli credentials should be configure in the root directory of your machine.

#### Installation Steps

- Clone the Repository
```
# clone repo
git clone https://github.com/protechanalysis/cde_capstone_project_24.git


# nagivate into the directory
cd cde_capstone_project_24
```

- Provision Infrastructure with Terraform
```
# nagivate into the directory
cd terraform

# terraform initialization
terraform init

# to deploy resources
terraform apply -- terraform statefile is managed backend

```

- Run Airflow Pipelines:

    -   Follow the instructions in the [airflow/readme.md](airflow/readme.md) to set up and trigger DAGs.

### DBT Models
Follow the instructions in the [dbt_postgres/readme.md](dbt_postgres/readme.md) to set up dbt.

### CI/CD Integration
File: .github/workflows/capstone.yml

- Code Linting: Ensures code follows best practices using flake8 and isort.
- Build and Push: Automates containerization and deployment to cloud container registry (docker hub).

### Infrastructure Provisioning
- Terraform provisions the following resources:
    - S3 bucket for storing raw data.
    - Amazon RDS for transformed data storage.
