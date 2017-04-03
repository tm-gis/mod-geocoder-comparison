# -*- coding: utf-8 -*-
"""
Script to compare performance of various geocoders.
Madeline Steele, TriMet, June 2016
"""

import geocoder, psycopg2, requests, json, time
from ott.geocoder.geosolr import GeoSolr
from unidecode import unidecode

tm_geocoder_url = 'http://maps.trimet.org/solr'
tm_geocoder = GeoSolr(tm_geocoder_url)

testAddr = '4012 SE 17th Ave, Portland'

# Mapzen bbox params
#boundary.rect.min_lon=-123.785578
#boundary.rect.min_lat=44.683870
#boundary.rect.max_lon=-121.651006
#boundary.rect.max_lat=46.059685

pdxlatlng = [45.5231, -122.6765]

## need to decide which geocoders to try
## google, geolytica, mapzen, osm, ....
## some, not all, need api keys
geocoderDict = {'mapzen':'search-jXrqfdD',
                'mapbox':'pk.eyJ1IjoibW9zdGVlbGUiLCJhIjoiY2lwZnA4cGF1MDAxM3RxbmowMm02anp2YSJ9.9uvwg81ec6Cikk7ECXTBEA',                
                #'geolytica':'',
                'google':'',
                'osm':'',
                'trimet':'',
                'arcgis':'',}

conn = psycopg2.connect("dbname=test user=postgres")
cur = conn.cursor()

## trimet_adds is a table of addresses with lat lngs "verified" by foursquare check-ins provided by
## Ervin Ruci in June 2016. A few rows have 0,0 for the latlng - we are dropping these

# copying a few rows to a test table for development purposes
cur.execute("DROP TABLE IF EXISTS geocoder_test")
cur.execute("CREATE TABLE geocoder_test AS SELECT * FROM trimet_adds WHERE lat != 0;") #" LIMIT 12;") #using LIMIT for testing
cur.execute("ALTER TABLE geocoder_test ADD COLUMN id SERIAL NOT NULL PRIMARY KEY;")

# This function works for all geocoders except trimet and mapzen
def getAddrAndLatLng(g):
    if ('ERROR' in g.status or g.status == 'ZERO_RESULTS'):
        addr = ''
        lat = 0
        lng = 0
    else:
        addr = unidecode(g.address.replace("'","\''"))      
        lat = g.lat
        lng = g.lng
    return addr, lat, lng
    
## this function copies address string results and the disntace in feet from "true location" to the table
## it converts lat lng numeric values to point geometry in local projection for distance calculation
def copyResultsToTable(anAddr, aLat,aLng):    
    updateSQL = "UPDATE geocoder_test SET " + aGeocoder + "_address=" + "'" + addr  + "', " + \
    aGeocoder + "_dist_ft=" \
    "ST_Distance(geom, ST_Transform(ST_SetSRID(ST_MakePoint(" + str(aLng) + "," + str(aLat) + "), 4326),2913))" + \
    " WHERE id=" + str(row[4]) + ";"

    cur.execute(updateSQL)

def parse_mapzen_response(txt):
    """
    `txt` is a string containing JSON-formatted text from Mapzen's API

    returns a dictionary containing the useful key/values from the most
       relevant result.
    """
    gdict = {} # just initialize a dict for now, with status of None
    data = json.loads(txt)
    if data['features']: # it has at least one feature...
        gdict['status'] = 'OK' 
        feature = data['features'][0] # pick out the first one
        props = feature['properties']  # just for easier reference
        gdict['confidence'] = props['confidence']
        gdict['label'] = unidecode(props['label']).replace("'","\''")

        # now get the coordinates
        coords = feature['geometry']['coordinates']
        gdict['longitude'] = coords[0]
        gdict['latitude'] = coords[1]
    else:
        gdict['label'] = ''
        gdict['longitude'] = 0
        gdict['latitude'] = 0
    return gdict

# adding blank results fields for each geocoder
for aGeocoder in geocoderDict.keys():
    sqlString = "ALTER TABLE geocoder_test ADD " + aGeocoder + "_address text, ADD " \
    + aGeocoder + "_dist_ft numeric;"
    cur.execute(sqlString)

counter = 0
#cur.execute("SELECT * FROM geocoder_test")
cur.execute("SELECT * FROM geocoder_test WHERE google_dist_ft IS NULL;")
for row in cur.fetchall():
    counter += 1
    if counter % 50 == 0:
        print counter, "addr:", row[0]
#    if counter == 20:
#        break

    for aGeocoder in geocoderDict.keys():
        if aGeocoder == 'trimet':
            gc = tm_geocoder.query(row[0], rows=1, start=0)
            results = gc.results[0]
            try:            
                addr = unidecode('{0}, {1}, OR'.format(results['name'].replace("'","\''"), results['city']))
            except:
                addr = ''
            lat = results['lat']
            lng = results['lon']

        ## I have to call mapzen outside of the geocoder wrapper because without the bbox parameters
        ## (which I can't pass through geocoder) it was returning some very wacky results - Kansas, Iowa, FL -
        ## and just in the first 10 records.
        elif aGeocoder == 'mapzen':
            request = "https://search.mapzen.com/v1/search?api_key=" + geocoderDict[aGeocoder] + "&text=" + row[0] + \
                    "&boundary.rect.min_lon=-123.785578&boundary.rect.min_lat=44.683870&boundary.rect.max_lon=-121.651006&boundary.rect.max_lat=46.059685"    
            resp = requests.get(request).text
            result = parse_mapzen_response(resp)
            addr = result['label']
            lng = result['longitude']
            lat = result['latitude']
                       
        else:
            this_geocoder = getattr(geocoder, aGeocoder)
            if geocoderDict[aGeocoder] == '':
                g = this_geocoder(row[0])
                while g.status == 'OVER_QUERY_LIMIT':
                    print "over", aGeocoder, "query limit - waiting for 30 minutes and trying again."
                    time.sleep(1800)
                    g = this_geocoder(row[0])
            else:
                ## another possible parameter to pass for mapbox is proximity=pdxlatlng (makes it
                ## favor results near that point) but it didn't improve things for a small sample 
                g = this_geocoder(row[0], key=geocoderDict[aGeocoder]) #

    
            addr, lat, lng = getAddrAndLatLng(g)
        copyResultsToTable(addr, lat, lng)

    conn.commit()
    
conn.close()


