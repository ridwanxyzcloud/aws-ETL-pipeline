import sys
import redshift_connector

# Connect to Redshift using the 'redshift_connector', create a cursor, and create tables using the extracted schema

conn = redshift_connector.connect(
    host='default-workgroup.339713018722.us-east-1.redshift-serverless.amazonaws.com',
    database='aws-etl-redshift',
    user='demo',
    password='password'
    )
conn.autocommit = True
cur = conn.cursor()

# Create tables
cur.execute('''
CREATE TABLE IF NOT EXISTS "dimDate" (
    "fips" INTEGER,
    "date" TIMESTAMP,
    "month" INTEGER,
    "year" INTEGER,
    "day_of_week" INTEGER
);  
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS "dimRegion" (
    "fips" REAL,
    "province_state" TEXT,
    "country_region" TEXT,
    "latitude" REAL,
    "longitude" REAL,
    "county" TEXT,
    "state" TEXT
);  
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS "dimHospital" (
    "fips" REAL,
    "state_name" TEXT,
    "latitude" REAL,
    "longitude" REAL,
    "hq_address" TEXT,
    "hospital_name" TEXT,
    "hospital_type" TEXT,
    "hq_city" TEXT,
    "hq_state" TEXT
);  
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS "factCovid" (
    "fips" REAL,
    "province_state" TEXT,
    "country_region" TEXT,
    "confirmed" REAL,
    "deaths" REAL,
    "recovered" REAL,
    "active" REAL,
    "date" INTEGER,
    "positive" REAL,
    "negative" REAL,
    "hospitalizedcurrently" REAL,
    "hospitalized" REAL,
    "hospitalizeddischarged" REAL
);  
''')

# Define the IAM role ARN
dwh_iam_role_arn = 'arn:aws:iam::339713018722:role/redshift-role'

# Define the COPY commands for each table
copy_factCovid = f"""
    COPY factCovid
    FROM 's3://covid19-lake-bucket/output-data/fact_covid.csv'
    IAM_ROLE '{dwh_iam_role_arn}'
    CSV
    IGNOREHEADER 1;
    """

copy_dimHospital = f"""
    COPY dimHospital
    FROM 's3://covid19-lake-bucket/output-data/dim_hospital.csv'
    IAM_ROLE '{dwh_iam_role_arn}'
    CSV
    IGNOREHEADER 1;
    """

copy_dimDate = f"""
    COPY dimDate
    FROM 's3://covid19-lake-bucket/output-data/dim_date.csv'
    IAM_ROLE '{dwh_iam_role_arn}'
    CSV
    IGNOREHEADER 1;
    """

copy_dimRegion = f"""
    COPY dimRegion
    FROM 's3://covid19-lake-bucket/output-data/dim_region.csv'
    IAM_ROLE '{dwh_iam_role_arn}'
    CSV
    IGNOREHEADER 1;
    """

# Execute the COPY commands
cur.execute(copy_factCovid)
cur.execute(copy_dimHospital)
cur.execute(copy_dimDate)
cur.execute(copy_dimRegion)

# Close cursor and connection
cur.close()
conn.close()

