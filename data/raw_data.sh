# After configuring AWS CLI 
# The commands below extracts data from the aws public data registry into your aws bucket
# NOTE: All required permisson must granted before for this operation to work

aws s3 sync s3://covid19-lake/enigma-jhu/ s3://covid19-lake-bucket/enigma-jhu/
aws s3 sync s3://covid19-lake/enigma-nytimes-data-in-usa/ s3://covid19-lake-bucket/enigma-nytimes-data-in-usa/
aws s3 sync s3://covid19-lake/rearc-covid-19-testing-data/ s3://covid19-lake-bucket/rearc-covid-19-testing-data/
aws s3 sync s3://covid19-lake/rearc-usa-hospital-beds/ s3://covid19-lake-bucket/rearc-usa-hospital-beds/
aws s3 sync s3://covid19-lake/static-datasets/ s3://covid19-lake-bucket/static-datasets/

