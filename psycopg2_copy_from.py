# requires 
# psycopg2 
# pscyopg2-binary 
# pandas

# psycopg2 docs
# https://www.psycopg.org/docs/
# see here for copy documentation
# https://www.psycopg.org/docs/usage.html#using-copy-to-and-copy-from

import io
import psycopg2
import psycopg2.extras
import pandas as pd


# this is the drawback to psycopg2: notice you have to put your username and
# pwd in plain text unless you come up with some other way to get data
con = psycopg2.connect(database='dbname',host='gp.host.address', 
                       user='username', port=5432, password="pwd")

# get data frame
file_df = pd.read_csv('../uthealth-dw/greenplum/datawarehouse/reference tables/cms_codes.csv')

# writes pandas data frame to a 'csv' but this csv is actually a buffer not a file
# notice seperator is tabs
f = io.StringIO()
file_df.to_csv(f, index=False, header=False,sep='\t')
f.seek(0)

# we create a cursor; that will connect to the database
# this will perform the copy command
with con.cursor() as cursor:
    # bug in psycopg2; that requires you to  set the schema
    # replace {schema} with the schema your table is in
    cursor.execute('set search_path to {schema}, public')
    # cursor now copies from f; to the table; put in the column names
    # if your only putting in some columns of the table; otherwise you
    # can leave it out; again notice the tab seperator
    cursor.copy_from(f, '{table}',sep='\t', columns=('cd_type', 'cd_value', 'initial_year', 'last_year', 'code_description'))