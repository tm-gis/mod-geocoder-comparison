# -*- coding: utf-8 -*-
"""
Script to compare performance of various geocoders.
Madeline Steele, TriMet, June 2016
"""

import psycopg2
from pandas import DataFrame

conn = psycopg2.connect("dbname=test user=postgres")
cur = conn.cursor()


distThresholdList = [25,50,100,200,250,500,1000]
distFieldList = ['Pct_closer_than_' + str(x) + 'ft' for x in distThresholdList]

def summarizeResults(outTableName, selectionQuery):
    createTableQuery = "CREATE TABLE " + outTableName + "(Geocoder text, " + ' numeric, '.join(distFieldList) + " numeric);"
    cur.execute("DROP TABLE IF EXISTS " + outTableName + ";")
    cur.execute(createTableQuery)
    
    cur.execute(selectionQuery)
    
    ## convert table to a pandas dataframe    
    the_data = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    df = DataFrame(the_data)
    df.columns = colnames
    
    rowCount = float(len(df))
    
    for column_name in df.columns:
        if "dist_ft" not in column_name:
            continue
        valuesStr = "'" + column_name.split('_')[0] + "'"
        for threshold in distThresholdList:
            pctInThreshold = ((df[column_name] < threshold).sum())/rowCount *100.0
            valuesStr = valuesStr + ', ' + str(pctInThreshold)
        insertQuery = "INSERT INTO " + outTableName + " VALUES(" + valuesStr + ");"  
        print insertQuery
        cur.execute(insertQuery)
    
# Running this twice - once for the all OR addresses in TriMet's 7-county AOI, and once just for metro  
sevenCountySelQuery = "SELECT * FROM geocoder_test;"
metroSelQuery = "SELECT * FROM geocoder_test as g JOIN metro_bnd AS m ON ST_contains(m.geom, g.geom);"

summarizeResults('geocoder_summary_seven_counties',sevenCountySelQuery)
summarizeResults('geocoder_summary_metro',metroSelQuery)

conn.commit()
    
cur.close()
conn.close()

