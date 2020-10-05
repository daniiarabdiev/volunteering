import psycopg2
from decouple import config
import pandas as pd
from utils import psql_insert_copy
from sqlalchemy import create_engine


conn = psycopg2.connect(host=config('database_host'), database=config('database_name'),
                        user=config('database_user'),
                        password=config('database_password'))

cur = conn.cursor()


# one table
with open('script.sql') as rfile:
    sql_script = rfile.read()

print(sql_script)
cur.execute(sql_script)

res = cur.fetchall()

cols = ['id', 'ad_account_id', 'created_at', 'dashboard_internal_id', 'modified_at', 'modified_by', 'status',
        'type', 'connection_id', 'user_id']

features_df = pd.DataFrame(res, columns=cols)

print(features_df)
##############

# Join

all_df = pd.concat([features_df for _ in range(2)])

print(all_df)

# train
#######

# push to database
engine = create_engine(f"postgresql://{config('database_username')}:{config('database_password')}@{config('database_host')}:{config('port')}/{config('database_name')}")
all_df.to_sql(config('table_name'), engine, schema=config('schema'), method=psql_insert_copy, if_exists='append', index=False)
