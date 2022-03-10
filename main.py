import os
from mmpac import *

# Load env variables 
verticaUser = os.getenv('verticaUser')
verticaPass = os.getenv('verticaPass')

# Setup vertica connection
print('Connecting to Vertica')
vertica_setup(server='aws_prod',                                     
              user=verticaUser,
              password=verticaPass,
              connection_timeout=3600)

# List of sql files to run.
# We could loop through all .sql files in the root folder, but this way we can manage the order of runs
extract_list = [
 'cp_special_path_retention_temp.sql'
  , 'req_test.sql'
]

# Query and store all extracts from extract_dict
for file_name in extract_list:
    print('Running Query {sql}'.format(sql = file_name))
    sql = open(file_name, 'r').read()
    # Removing commented out DROP AND CREATE statements from sql
    adjusted_sql = sql.replace('/*', '').replace('*/', '')
    vsql(cmd = adjusted_sql)
