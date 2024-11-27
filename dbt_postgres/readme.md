### DBT Documentation

This DBT project transforms raw country demographic data into a fact and dimensional model.

```
# navigate into dbt directory
cd dbt_postgres

# creating virtual environment
python3 -m venv dbt_env

# activate virtual environment
source dbt_env/bin/activate

# instal dbt dependencies
pip install -r requirement-dbt.txt

# initiate dbt project
dbt init dbt_project
```

### Configure dbt profile
###### ~/.dbt/profiles.yml
```
dbt_project:
  outputs:
    dev:
      dbname: rds_database_name
      host: your_rds_endpoint
      pass: your_rds_password
      port: 5432
      schema: public
      threads: 3
      type: postgres
      user: your_rds_username
  target: dev
```

### Test connection
```
dbt debug --- # connection must pass
```

### Run all models
```
dbt run
```

### DATA MODEL
#### Entity Relationship Diagram

![relationshipflow](/images/erd.png)
