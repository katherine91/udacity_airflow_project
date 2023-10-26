#### 1. create IAM user in AWS with S3 and Redshift access, create access key ####
#### 2. load data to S3 ####
#### 3. create redshit serverless with role that has policies of S3 access ####
#### 4. in airflow create two connections for redshift and AWS ####
#### 5. create all needed tables directly in redshift or through python code or airflow dag - create_tables.py ####
#### 6. copy data from s3 to redshift - StageToRedshiftOperator class ####
#### 7. load data to fact table - LoadFactOperator class ####
#### 8. load data to dimension table LoadDimensionOperator class ####
#### 9. Do quality checks DataQualityOperator class ####