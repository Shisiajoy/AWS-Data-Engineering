## AWS-Data-Engineering 
    RICK AND MORTY DATABASE ETL


### PROJECT OVERVIEW

This project demonstrates an ETL (Extract, Transform, Load) to extract data from the Rick and Morty API, transform it into a structured format, and load it into an AWS RDS MySQL database for querying. It leverages AWS Lambda, S3, and RDS, with Python scripts to handle the ETL process.
The final product allows one to perform insightful queries and visualizations of the show's characters, episodes, and locations using SQL.

#### 🎯 KEY OBJECTIVES :

1.Extract data from the Rick and Morty API.

2.Transform the data into a format suitable for storage and analysis.

3.Load the transformed data into an AWS RDS MySQL instance for querying and analysis.

4.Create a robust and scalable ETL solution using AWS services and Python.

#### PROJECT ARCHITECTURE.
 ##### ***Tools & Services Used.***

- AWS Lambda: For orchestrating the ETL process.
  
- AWS S3: Used for file storage and staging.

- AWS RDS (MySQL): The destination for cleaned data.
  
- HeidiSQL: To manage and query the MySQL database.
  
- Python: To develop Lambda functions and ETL scripts.


#### ETL Workflow:


![RICK AND MORTY ETL](https://github.com/user-attachments/assets/901875fd-3a93-482d-843b-ad2cf22923af)


1.API Extraction:

- Lambda functions extract data from the Rick and Morty API and temporarily store it in AWS S3.

2.Data Transformation:

- Another Lambda function retrieves the data from S3, normalizes it, and prepares it for loading into RDS.

3.Loading into AWS RDS:

- The transformed data is then loaded into an AWS RDS MySQL database, where it can be queried for analytics.

#### 💻Setup & Installation
1.Requirements:
- AWS Account with Lambda, S3, and RDS setup.

- Python 3.8 or higher.
  
- SQL Client: HeidiSQL (or any preferred SQL client).

2. Install dependencies.
3. Set up AWS credentials to allow accessto S3,Lambda and RDS services.

#### Running ETL

1. Deploy Lambda Functions:
Upload the provided Lambda function to AWS Lambda.

2. S3 File Management:
Use s3_file_operations.py to interact with S3 bucket for staging data.

3. Load Data into RDS:
Trigger the Lambda function to load the data into AWS RDS.

4. Querying the Data:
Use HeidiSQL or any other SQL client to connect to the RDS instance and run SQL queries.


#### Verifying Data Load in AWS RDS
- Once the ETL pipeline is completed,verify the data load by:

1.Connecting to your AWS RDS instance using HeidiSQL or any SQL client.

2.Running sample queries (e.g., SELECT * FROM Character_Table LIMIT 10;) to check the data.

#### 🚧FUTURE IMPROVEMENTS.
- Automating ETL Triggers with AWS EventBridge
  
- Data Visualization using a dashboard tool like Amazon QuickSight.





 
 
