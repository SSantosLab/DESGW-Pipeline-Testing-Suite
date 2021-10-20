import pandas as pd
import psycopg2
import numpy as np
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--exp_list', type=str)
args = parser.parse_args()

df = pd.DataFrame(columns=['exposure', 'nite', 'radeg', 'decdeg'])

conn =  psycopg2.connect(database='decam_prd',user='decam_reader',host='des61.fnal.gov',port=5443)

e = open(args.exp_list,'r')
el = e.readlines()
eList = [exp.strip() for exp in el]
e.close()

for exp in eList:

    query = """SELECT id as EXPOSURE,
    TO_CHAR(date - '12 hours'::INTERVAL, 'YYYYMMDD') AS NITE,
    ra AS RADEG,
    declination AS DECDEG,
    filter as BAND
    FROM exposure.exposure
    WHERE id ="""+exp+""" ORDER BY id"""
    
    exposure_data = pd.read_sql(query, conn)#,index_col ="exposure")
    
    df = pd.concat([df,exposure_data]) # Add exposure_data dataframe to overhead dataframe

conn.close()
df = df.set_index('exposure', drop = True)
df.to_csv('exp_list_full.list')
