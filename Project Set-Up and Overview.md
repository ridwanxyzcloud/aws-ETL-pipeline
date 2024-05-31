### Index 0: AWS Cli Configuration

- configure your aws cli to 
Navigate to your aws-cli config file and set your access and sceceret key 
cd ~/.aws
nano config

- OR : 


aws configure

* Input access and secrete key

### Step 1: Create S3 Bucket 

Create a bucket on your aws s3


aws s3 mb s3://covid-lake-bucket



### step 2: Download raw data to s3 bucket
copy the covid data from the aws data registry

##### this code extracts all the data directories intended for use in the project
aws s3 sync s3://covid19-lake/ s3://covid19-lake-bucket/ --exclude "*" --include "enigma-jhu/*" --include "enigma-nytimes-data-in-usa/*" --include "rearc-covid-19-testing-data/*" --include "rearc-usa-hospital-beds/*" --include "static-datasets/*"


# OR
aws s3 sync s3://covid19-lake/enigma-jhu/ s3://covid19-lake-bucket/enigma-jhu/
aws s3 sync s3://covid19-lake/enigma-nytimes-data-in-usa/ s3://covid19-lake-bucket/enigma-nytimes-data-in-usa/
aws s3 sync s3://covid19-lake/rearc-covid-19-testing-data/ s3://covid19-lake-bucket/rearc-covid-19-testing-data/
aws s3 sync s3://covid19-lake/rearc-usa-hospital-beds/ s3://covid19-lake-bucket/rearc-usa-hospital-beds/
aws s3 sync s3://covid19-lake/static-datasets/ s3://covid19-lake-bucket/static-datasets/

# OR : touch the command into a bash file name 'sync_script.sh' 
# make script executable
chmod +x sync_script.sh

# exceute script using CLI

!bash raw_data.sh

![https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/data/raw_data.sh]

### step 3: Create and configure Crawler 

After getting the data in s3 bucket, use crawler get the full understanding of the data so you can build a data model 
- Create a crawler 
- include your file path to be inside the folder the data is located. not path of the data itself, but of the folder holding it 
- include all  folders for data to be crawled
- create role to allow full service access 
- create a database for it or add a database if you dont already have one 
- run on demand

![Crawler](https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/snapshots/crawler.png)


NOTE: It is very important to attach role for every data source indivually or collectively. All crawler targets must be assigned a role . Otherwise, it will not recognise added folders .

### step 4: Run crawler and extract metadata
Run the crawler

![tables extracted by crawler](https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/snapshots/crawler_tables.png)

### step  5: Configure Athena 
Open athena Query and set the Query result location and encryption to a new bucket that will hold meta data 

![athena query result location](https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/snapshots/athena_query_location.png)


### step 6: Query Extracted Metadata
In Athena, All generated table can be queried to see the table structure. 
This structure is used to better understand the data and generate the project's data models.

### step 7: Generate DDL for Data Model
Generate the DDL sql and use it to create your data model and subsequently your data warehouse.
- The relational data model

![relational data model](https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/data%20model/covid19-project-1NF.drawio.png)

- Dimensional and fact Data Model for the aws redshift data warehouse 
![data warehouse model](https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/data%20model/covid19_DW_2NF.drawio.png)

### step 8: Extract and Tranformed raw data 
Connect to athena and s3 on a notebook to query and extract the data into dataframes using data structures gotten by the crawler.

![athena and s3 query and extraction](https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/snapshots/athena_s3_query.png)

- Tranform the data to extract the dimensional and fact table needed for the data warehouse.


- Staging (Load the output data into s3 bucket).

![Buffered DW csv to S3](https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/snapshots/buffred_csv_to_s3.png)

* transfered csv in s3 bucket in 'output-data' directory
![](https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/snapshots/DW_csv_files_in_s3_bucket.png)

### step 9: DataFrame Schema Extraction using pandas 
Use pandas to extract the schema of the prepared dataframes
- use the schema details to create table on redshift.

![schema extraction](https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/snapshots/schema_extraction.png)

### step10: Configure Redshift
AWS Redshift does not use cluster like they used to, it is severless and uses workgroup and namespace.

- The namespace url is what will be your host for the datawarehouse 

- NOTE: permision must be attached to the namespace to allow an service or transaction to s3 or other services

![namespace configurations](https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/snapshots/namespace_1.png)

![](https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/snapshots/namespace_2.png)

### step11: Run ETL Job in AWS Glue

Run ETL job in python using AWS Glue
- the job script creates table on redshift using extracted schema
- An additional python library (redshift-connector) is used to connect to redshift in the script.(make sure to download reshift-connector file, save it in a folder on s3 and add the path where it is located as your python library)

- the job loads the data into redshift data warehouse using the COPY command 

(aws glue job script)[https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/55174b97286a6df90e3abf90ee4457a9e3560da9/src/aws_glue_jobs/glue_jobs.py]

![redshift-connector path](https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/snapshots/redshift-connector-path.png)

![datawarehouse](https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/snapshots/reshfit-data-warehouse%20.png)

### step 12: Connect Reshift to BI Tool 

the redshift data warehouse housing the transformed and final data  can then be connected to any BI and Visualization tool for Insights. 

### Resources and Scripts

    * This project has an interactive python notebook that can be used in a python environment to properly see a step by step process of the project

[https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/project-notebook.ipynb]

    * AWS Glue Job Script

[aws glue job script][https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/55174b97286a6df90e3abf90ee4457a9e3560da9/src/aws_glue_jobs/glue_jobs.py]


    * This project also use config parser to fetch sensitive details saved in Config files. Another option you can use is an environment variable