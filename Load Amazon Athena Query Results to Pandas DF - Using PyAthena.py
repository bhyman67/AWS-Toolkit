from pyathena import connect
import pandas as pd
from pyathena.pandas.util import as_pandas

conn = connect(
    profile_name='',
    work_group='', 
    region_name='us-east-1'
)

query = '''
    select *
    from __.__
'''

# df_data = pd.read_sql_query(query, conn)
# print(df_data)

# This could potentially be a faster way... 
cursor = conn.cursor()
cursor.execute(query)
df = as_pandas(cursor)
print(df.describe())

print("Done")