# Import required libraries 
import boto3
import pandas as pd
from io import StringIO # python3: python2: BytesIO
import configparser 
import time

# # Read the config file
config = configparser.ConfigParser()
config.read('https://github.com/ridwanxyzcloud/aws-ETL-pipeline/blob/main/config/config.ini')

# Extract configuration details
aws_access_key = config['aws']['AWS_ACCESS_KEY']
aws_access_secret = config['aws']['AWS_ACCESS_SECRET']
aws_region = config['aws']['AWS_REGION']
schema_name = config['aws']['SCHEMA_NAME']
s3_staging_dir = config['aws']['S3_STAGING_DIR']
s3_bucket_name = config['aws']['S3_BUCKET_NAME']
s3_output_directory = config['aws']['S3_OUTPUT_DIRECTORY']
password = config['aws']['PASSWORD']
host = config['aws']['HOST']
port = config['aws']['PORT']
dwh_iam_role_arn = config['aws']['DWH_IAM_ROLE_ARN']


# Create the Athena client
athena_client = boto3.client(
    'athena',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_access_secret,
    region_name=aws_region
)

# Athena Query function

def execute_athena_query(query, database, output_location):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )
    return response

# Function to get Query Result

def get_query_results(query_execution_id):
    import time
    
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            break
        elif state == 'FAILED':
            raise Exception("Query failed")
        elif state == 'CANCELLED':
            raise Exception("Query was cancelled")
        time.sleep(2)
    result_response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    return result_response

# -----------------------------------------------------------------------
# This function is defined to download the query results from Athena
# It takes two parameters 'client: boto3.client; and response

Dict = {}
def download_and_query_results(client: boto3.client, query_response: Dict) -> pd.DataFrame:
    while True:
        try:
            # This function only loads the first 1000 rows
            client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err
    
    temp_file_location: str = "athena_query_results.csv"
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_access_secret,
        region_name=aws_region,
    )
    s3_client.download_file(
        s3_bucket_name,
        f"{s3_output_directory}/{query_response['QueryExecutionId']}.csv",
        temp_file_location,
    )
    return pd.read_csv(temp_file_location) 


## ** Data Extraction into DataFrames and confirmatio ** 


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM csv", 
    QueryExecutionContext={"Database": schema_name},
    ResultConfiguration={
        "OutputLocation": s3_staging_dir, 
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}, 
    },
)
response
# 'enigma_jhu' DataFrame
enigma_jhu = download_and_query_results(athena_client, response)
enigma_jhu.head()
enigma_jhu.shape

# 'us_county' DataFrame
response = athena_client.start_query_execution(
    QueryString="SELECT * FROM us_county", 
    QueryExecutionContext={"Database": schema_name},
    ResultConfiguration={
        "OutputLocation": s3_staging_dir, 
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}, 
    },
)
us_county = download_and_query_results(athena_client, response)
us_county.shape

# 'us_states' DataFrame
response = athena_client.start_query_execution(
    QueryString="SELECT * FROM us_states", 
    QueryExecutionContext={"Database": schema_name},
    ResultConfiguration={
        "OutputLocation": s3_staging_dir, 
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}, 
    },
)
us_states = download_and_query_results(athena_client, response)
us_states.shape

# 'static_state_abv' DataFrame
response = athena_client.start_query_execution(
    QueryString="SELECT * FROM state_abv", 
    QueryExecutionContext={"Database": schema_name},
    ResultConfiguration={
        "OutputLocation": s3_staging_dir, 
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}, 
    },
)
static_state_abv = download_and_query_results(athena_client, response)
static_state_abv.shape

# 'static_countrycode' DataFrame
response = athena_client.start_query_execution(
    QueryString="SELECT * FROM countrycode", 
    QueryExecutionContext={"Database": schema_name},
    ResultConfiguration={
        "OutputLocation": s3_staging_dir, 
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}, 
    },
)
static_countrycode = download_and_query_results(athena_client, response)
static_countrycode.shape

# 'static_countypopulation' DataFrame
response = athena_client.start_query_execution(
    QueryString="SELECT * FROM countypopulation", 
    QueryExecutionContext={"Database": schema_name},
    ResultConfiguration={
        "OutputLocation": s3_staging_dir, 
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}, 
    },
)
static_countypopulation = download_and_query_results(athena_client, response)
static_countypopulation.shape

# 'rearc_us_daily' DataFrame
response = athena_client.start_query_execution(
    QueryString="SELECT * FROM us_daily", 
    QueryExecutionContext={"Database": schema_name},
    ResultConfiguration={
        "OutputLocation": s3_staging_dir, 
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}, 
    },
)
rearc_us_daily = download_and_query_results(athena_client, response)
rearc_us_daily.shape

# 'rearc_states_daily' DataFrame
response = athena_client.start_query_execution(
    QueryString="SELECT * FROM states_daily", 
    QueryExecutionContext={"Database": schema_name},
    ResultConfiguration={
        "OutputLocation": s3_staging_dir, 
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}, 
    },
)
rearc_states_daily = download_and_query_results(athena_client, response)
rearc_states_daily.shape

# 'rearc_us_hospital_beds' DataFrame
response = athena_client.start_query_execution(
    QueryString="SELECT * FROM json", 
    QueryExecutionContext={"Database": schema_name},
    ResultConfiguration={
        "OutputLocation": s3_staging_dir, 
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}, 
    },
)
rearc_us_hospital_beds = download_and_query_results(athena_client, response)
rearc_us_hospital_beds.shape


# 'rearc_us_total_latest' DataFrame
response = athena_client.start_query_execution(
    QueryString="SELECT * FROM us_total_latest", 
    QueryExecutionContext={"Database": schema_name},
    ResultConfiguration={
        "OutputLocation": s3_staging_dir, 
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}, 
    },
)
rearc_us_total_latest = download_and_query_results(athena_client, response)
rearc_us_total_latest.shape

# Data Transformation for one of the extracted DataFrames
# use 'iloc' slicing to change the column header 

new_header = static_state_abv.iloc[0]

# assign it to the dataframe columns so the change is saved
static_state_abv.columns = new_header

static_state_abv.columns


## ** Extraction of dimensional table and Fact Table DataFrames ** 

#fact_covid dataFrame 
factCovid_1 = enigma_jhu[['fips','province_state','country_region','confirmed','deaths','recovered','active']]
factCovid_2 = rearc_states_daily[['fips','date','positive','negative','hospitalizedcurrently','hospitalized','hospitalizeddischarged']]
#merge the two derivative tables
fact_covid = pd.merge(factCovid_1, factCovid_2, on='fips', how='inner')

fact_covid.shape

#dim_region dataFrame 
dimRegion_1 = enigma_jhu[['fips','province_state','country_region','latitude','longtitude']]
dimRegion_2 = us_county[['fips','county','state']]
#merge 
dim_region = pd.merge(dimRegion_1,dimRegion_2, on='fips', how='inner')

#dim_hospital dataFrame 
dim_hospital = rearc_us_hospital_beds[['fips','state_name','latitude','longtitude','hq_address','hospital_name','hospital_type','hq_city','hq_state']]
dim_hospital.shape

#dim_date dataFrame 
dim_date = rearc_states_daily[['fips','date']]

# transform 'dim_date' to extract needed data and columns
#split 
# Convert the 'date' column from int64 datatype to datetime format
dim_date['date'] = pd.to_datetime(dim_date['date'], format='%Y%m%d')
dim_date['year'] = dim_date['date'].dt.year
dim_date['month'] = dim_date['date'].dt.month
dim_date['day_of_week'] = dim_date['date'].dt.dayofweek

# redefine the columns and assign back to 'dim_date'
dim_date = dim_date[['fips', 'date', 'month', 'year', 'day_of_week']]

# confirm transformation
dim_date.shape
dim_date.info()


## ** Save all transformed DataFrame to S3 bucket after csv buffering ** 

# saving collectively
# DataFrames to save

data_frames = {
    'fact_covid': fact_covid,
    'dim_hospital': dim_hospital,
    'dim_region': dim_region,
    'dim_date': dim_date
}
# Create an S3 resource
s3 = boto3.resource('s3')

# Upload CSV files to S3 after buffering to binary format
for df_name, df in data_frames.items():
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.Object(s3_bucket_name, f"{s3_output_directory}/{df_name}.csv").put(Body=csv_buffer.getvalue())

print(f"DataFrames successfully uploaded to {s3_output_directory} on Amazon S3.")

# OR use this code 
### NOTE: The second code is faster and use less resources. 

## ********************************************* ##

# Create S3 resource
s3 = boto3.resource('s3')

# Function to upload DataFrame to S3
def save_df_to_s3(df, bucket_name, file_path):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.Object(bucket_name, file_path).put(Body=csv_buffer.getvalue())

# 'fact_covid' DataFrame to S3 
save_df_to_s3(fact_covid, s3_bucket_name, f"{s3_output_directory}/fact_covid.csv")
print(f"DataFrames successfully uploaded to {s3_output_directory} on Amazon S3.")

# 'dim_hospital' DataFrame to S3 
save_df_to_s3(dim_hospital, s3_bucket_name, f"{s3_output_directory}/dim_hospital.csv")
print(f"DataFrames successfully uploaded to {s3_output_directory} on Amazon S3.")


# 'dim_region' DataFrame to S3 

save_df_to_s3(dim_region, s3_bucket_name, f"{s3_output_directory}/dim_region.csv")
print(f"DataFrames successfully uploaded to {s3_output_directory} on Amazon S3.")

# 'dim_date' DataFrame to S3 

save_df_to_s3(dim_date, s3_bucket_name, f"{s3_output_directory}/dim_date.csv")
print(f"DataFrames successfully uploaded to {s3_output_directory} on Amazon S3.")

## ** Extract DataFrame Schema to create DataWarehouse on Redshift

# extract DataFrame Schema
fact_covid_sql = pd.io.sql.get_schema(fact_covid.reset_index(), 'factCovid')
dim_hospital_sql = pd.io.sql.get_schema(dim_hospital.reset_index(), 'dimHospital')
dim_region_sql = pd.io.sql.get_schema(dim_region.reset_index(), 'dimRegion')
dim_date_sql = pd.io.sql.get_schema(dim_date.reset_index(), 'dimDate')

print(''.join(fact_covid_sql))
print(''.join(dim_hospital_sql))
print(''.join(dim_region_sql))
print(''.join(dim_date_sql))

## ** The loading part of project is done using AWS Glue
# Check aws_glue_job directory for the glue_jobs.py script.