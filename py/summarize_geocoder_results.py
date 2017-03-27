# -*- coding: utf-8 -*-
'''
Run this script after running 'geocoder_comp_2017.py' (in same dir)
'''

import psycopg2
import matplotlib.pyplot as plt
import numpy as np
from pandas import DataFrame
import image

# Accessing the database with geocoder testing results
db_cred_file = 'P:/geocoder_db_creds.txt'
db_creds = open(db_cred_file, 'r').read().strip()
conn = psycopg2.connect(db_creds)
cur = conn.cursor()

# Running this twice - once for the all OR addresses in TriMet's 7-county AOI, and once just for metro
sevenCountySelQuery = "SELECT * FROM geocoder_test;"
# metroSelQuery = "SELECT * FROM geocoder_test as g JOIN metro_bnd AS m ON ST_contains(m.geom, g.geom);"
cur.execute(sevenCountySelQuery)
the_data = cur.fetchall()
col_names = [desc[0] for desc in cur.description]
df = DataFrame(the_data)
df.columns = col_names
df.convert_objects(convert_numeric=True)

cur.close()
conn.close()

distThresholdList = range(0, 501, 5)
# distThresholdList = [25, 50, 100, 200, 250, 500, 1000]
distFieldList = ['Pct_closer_than_' + str(x) + 'ft' for x in distThresholdList]
geocoders = [col.split('_')[0] for col in df.columns if 'dist_ft' in col]
results_df = DataFrame(index=geocoders, columns=distFieldList)

for dist in distThresholdList:
    for geocoder in geocoders:
        geo_dist_field = geocoder + '_dist_ft'
        dist_field = 'Pct_closer_than_' + str(dist) + 'ft'
        no_nulls = df[geo_dist_field].astype('float').dropna()
        pct_in_threshold = len(no_nulls[no_nulls < dist]) / float(len(df)) * 100.0
        results_df.set_value(geocoder, dist_field, pct_in_threshold)


dist_array = np.array(distThresholdList)

# From Carto Colors: https://carto.com/carto-colors
colors = ['#7F3C8D', '#11A579', '#3969AC', '#F2B701', '#E73F74', '#80BA5A', '#E68310',
          '#008695', '#CF1C90', '#f97b72', '#4b4b8f', '#A5AA99']

fig = plt.figure(figsize=(11, 8))
ax = plt.gca()
ax.set_xlabel("Distance from verified location (ft)")
ax.set_ylabel("Percent of geocoder results within distance threshold")
plt.ylim((0,100))
i = 0
for geocoder in geocoders:
    line_color = colors[i]
    ax.plot(dist_array, results_df.loc[geocoder], label=geocoder, color=line_color,
            linewidth=3) #, marker='x')
    i += 1

plt.legend(loc=4, frameon=False)
# plt.show()

plt.savefig('foursquare_geocoder_comp.jpg')
