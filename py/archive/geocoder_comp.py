# -*- coding: utf-8 -*-
"""
Script to compare performance of various geocoders.
Madeline Steele, TriMet, June 2016
"""

import geocoder, psycopg2
from ott.geocoder.geosolr import GeoSolr

tm_geocoder_url = 'http://maps.trimet.org/solr'
tm_geocoder = GeoSolr(tm_geocoder_url)

search = '4012 SE 17th Ave'
gc = geocoder.query(search, rows=1, start=0)
results = gc.results[0]

print 'Coordinate Info:'
print '{0}, {1}'.format(results['lon'], results['lat'])
print '{0}, {1}'.format(results['x'], results['y'])
print '\nAddress Info:'
print '{0}, {1}'.format(results['name'], results['city'])

## need to decide which geocoders to try
## google, geolytica, mapzen, osm, ....
geocoderDict = {'mapzen':'search-jXrqfdD',
                'mapbox':'pk.eyJ1IjoibW9zdGVlbGUiLCJhIjoiY2lwZnA4cGF1MDAxM3RxbmowMm02anp2YSJ9.9uvwg81ec6Cikk7ECXTBEA',                
                'geolytica':'',
                'google':'',
                'osm':'',
                'arcgis':''}

conn = psycopg2.connect("dbname=test user=postgres")
cur = conn.cursor()

# this is a table of addresses with lat lngs "verified" by foursquare check-ins provided by
# Ervin Ruci in June 2016. A few rows have 0,0 for the latlng - we are dropping these
#cur.execute("SELECT * FROM oregon WHERE lat != 0 LIMIT 10;") #using LIMIT for testing

#copying a few rows to a test table for development purposes
cur.execute("DROP TABLE IF EXISTS geocoder_test")
cur.execute("CREATE TABLE geocoder_test AS SELECT * FROM oregon WHERE lat != 0 LIMIT 4;") #using LIMIT for testing
cur.execute("ALTER TABLE geocoder_test ADD COLUMN id SERIAL NOT NULL PRIMARY KEY;")

for aGeocoder in geocoderDict.keys():
    sqlString = "ALTER TABLE geocoder_test ADD " + aGeocoder + "_address text, ADD " \
    + aGeocoder + "_lat numeric, ADD " + aGeocoder + "_lng numeric;"
    print sqlString    
    cur.execute(sqlString)


cur.execute("SELECT * FROM geocoder_test")

def copyGeocoderOutputsToTable(g):
    if g.lat is None:
        aLat = 0
        aLng = 0
    else:
        aLat = g.lat
        aLng = g.lng
    updateSQL = "UPDATE geocoder_test SET " + aGeocoder + "_address=" + "'" + str(g.address) + "', " + \
    aGeocoder + "_lat='" + str(aLat) + "'," + aGeocoder + "_lng='" + str(aLng) + \
    "' WHERE id=" + str(row[3]) + ";"
    
    cur.execute(updateSQL)

for row in cur.fetchall():
    print row[0], row[3]
    for aGeocoder in geocoderDict.keys():
        this_geocoder = getattr(geocoder, aGeocoder)
        if geocoderDict[aGeocoder] == '':
            g = this_geocoder(row[0])
        else:
            print aGeocoder, 'has an api key:', geocoderDict[aGeocoder]
            g = geocoder.mapzen(row[0], key=geocoderDict[aGeocoder])            
        copyGeocoderOutputsToTable(g)


conn.commit()
conn.close()



