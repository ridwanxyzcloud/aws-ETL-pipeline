
# error encountered when trying to load using copy command
ERROR: Load into table 'factcovid' failed. Check 'sys_load_error_detail' system table for details. [ErrorId: 1-6658bc16-3edf08850a7e1702660a3179]

# sys_load_error 
# this error was checked using the query below to extract a detailed error log 

SELECT * FROM sys_load_error_detail
       

#OUTPUT 

ERROR: permission denied for relation stl_load_errors [ErrorId: 1-6658a415-6234b663334f6605303026ce]

Error accessing the database "awsdatacatalog"

The current user is not authenticated with IAM credentials. Edit your connection to the workgroup and choose "Federated user". Learn more 

# Change to federated user on sql workbench


# SOLUTION 
#Grant access to user 
## Connect to the correct database as admin or user with right priviledege and execute these commands
GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO admin;
GRANT SELECT ON TABLE stl_load_errors TO admin;
GRANT SELECT ON TABLE svv_table_info TO admin;


# Rerun query again 
SELECT * FROM sys_load_error_detail

# Error Message retrieved 
"Invalid digit, Value '.', Pos 2, Type = Integer"

# This error often occurs when the data in the CSV file does not match the expected data type defined in the table schema.