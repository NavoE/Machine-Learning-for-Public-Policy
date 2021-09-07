# -*- coding: utf-8 -*-
"""
Created on Mon Sep  6 20:55:42 2021

@author: Navo
"""
import pandas as pd
import json
import requests
from sqlalchemy import create_engine
import io

# Call API data
url= 'https://api.census.gov/data/2019/acs/acs5?get=NAME,GEO_ID,B01003_001E,B01002_001E&for=block%20group:*&in=state:42%20county:*'
response = requests.get(url)
if response.status_code== 200:
    data = json.loads(response.content.decode('utf-8'))

# Create data frame
Data = pd.DataFrame(data[1:], columns=data[0])
Data = Data.rename(columns={"NAME": "LocationDetails", "GEO_ID": "GEO_ID", "B01003_001E": "Population", "state": "State", "county": "County", "tract": "CensusTract", "block group": "BlockGroup"})

#Create connection to database, save dataframe to csv, and export to PostgreSQL
engine = create_engine('postgresql+psycopg2://mlpp_student:CARE-horse-most@acs-db.mlpolicylab.dssg.io:5432/acs_data_loading', connect_args={'options': '-csearch_path={}'.format('acs')})
Data.head(0).to_sql('nemmanue_acs_data', engine, if_exists='replace',index=False) #drops old table and creates new empty table
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
Data.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
contents = output.getvalue()
cur.copy_from(output, 'nemmanue_acs_data', null="") # null values become ''
conn.commit()
cur.close()



























