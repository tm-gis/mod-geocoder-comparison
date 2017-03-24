# -*- coding: utf-8 -*-
"""
Script to compare performance of various geocoders.
Madeline Steele, TriMet
"""

import geocoder, psycopg2, requests, json, time
from ott.geocoder.geosolr import GeoSolr
from unidecode import unidecode

tm_geocoder_url = 'http://maps.trimet.org/solr'
tm_geocoder = GeoSolr(tm_geocoder_url)

testAddr = '4012 SE 17th Ave, Portland'

'''
To Do: Add RLIS geocoder
'''
# dict formatted text file of geocoders with API keys
geocoder_file = 'P:/geocoder_dict.txt'

# text file with db credentials
db_cred_file = 'P:/geocoder_db_creds.txt'

# For Mapzen bbox parameter
bbox = "&boundary.rect.min_lon=-123.785578" \
        + "&boundary.rect.min_lat=44.683870" \
        + "&boundary.rect.max_lon=-121.651006" \
        + "&boundary.rect.max_lat=46.059685"

pdxlatlng = [45.5231, -122.6765]


def getAddrAndLatLng(g):
    """
    This function works for all geocoders except trimet and mapzen
    """
    if ('ERROR' in g.status or g.status == 'ZERO_RESULTS'):
        address = ''
        latitude = 0
        longitude = 0
    else:
        address = unidecode(g.address.replace("'", "\''"))
        latitude = g.lat
        longitude = g.lng
    return address, latitude, longitude
    

def copyResultsToTable(anAddr, aLat, aLng):
    """
    this function copies address string results and the distance in feet
    from "true location" to the table it converts lat lng numeric values
    to point geometry in local projection for distance calculation
    """
    if aLng != 'Null':
        updateSQL = "UPDATE geocoder_test SET " + aGeocoder + "_address='" \
                    + anAddr + "', " + aGeocoder + "_dist_ft=" \
                    "ST_Distance(geom, ST_Transform(ST_SetSRID(ST_MakePoint(" \
                    + str(aLng) + "," + str(aLat) + "), 4326),2913))" \
                    " WHERE id=" + str(row[4]) + ";"
    else:
        updateSQL = "UPDATE geocoder_test SET " + aGeocoder + "_address='" \
                    + anAddr + "', " + aGeocoder \
                    + "_dist_ft='NaN' WHERE id=" + str(row[4]) + ";"
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


def rlis_geocode(addr_str, token):
    """Take an input address string, send it to the rlis api and return
    a dictionary that are the state plane coordinated for that address,
    handle errors in the request fails in one way or another. Copied 
    from Grant Humphries's 'geocode_employer_records.py' (on github)"""

    url = 'http://gis.oregonmetro.gov/rlisapi2/locate/'
    params = {
        'token': token,
        'input': addr_str,
        'form': 'json'
    }
    rsp = requests.get(url, params=params)

    if rsp.status_code != 200:
        print 'unable to establish connection with rlis api'
        print 'status code is: {0}'.format(rsp.status_code)
        return None

    json_rsp = rsp.json()
    if json_rsp['error']:
        print 'the rlis api could not geocode this address'
        return None
    else:
        return json_rsp['data'][0]


def strip_addresses_of_zip_country(address_string):
    # remove spaces between commas
    address_string = address_string.replace(", ", ",")
    address_list_comma = address_string.split(",")

    # remove country
    country_candidate = address_list_comma[len(address_list_comma) - 1]
    country_strings = ["United States", "US", "USA", "united states", "us", "usa"]
    if country_candidate in country_strings:
        address_list_comma.remove(country_candidate)

    # remove zip
    zip_candidate = address_list_comma[len(address_list_comma) - 1].upper()
    if zip_candidate.find("OR") > -1 or zip_candidate.find("WA") > -1:
        zip_candidate = zip_candidate.replace("OR", "").replace("WA", "").replace(" ", "")
    if zip_candidate.find("-") > -1:
        zip_code_five, zip_code_four = zip_candidate.split("-")
        if len(zip_code_five) == 5 and len(zip_code_four) == 4 and zip_code_five.isdigit() and zip_code_four.isdigit():
            address_list_comma[len(address_list_comma) - 1] = \
                address_list_comma[len(address_list_comma) - 1].replace(zip_candidate, "").replace(" ", "")
    if len(zip_candidate) == 5 and zip_candidate.isdigit():
        address_list_comma[len(address_list_comma) - 1] = \
            address_list_comma[len(address_list_comma) - 1].replace(zip_candidate, "").replace(" ", "")
    return ", ".join(a for a in address_list_comma)

# some geocoders need api keys
with open(geocoder_file, 'r') as inf:
    geocoder_dict = eval(inf.read())

db_creds = open(db_cred_file, 'r').read().strip()

conn = psycopg2.connect(db_creds)
cur = conn.cursor()

# Trimet_adds is a table of addresses with lat lngs "verified" by foursquare
# check-ins provided by Ervin Ruci in June 2016. A few rows have 0,0 for the
# latlng - we are dropping these

# Careful with this line!
cur.execute('DROP TABLE IF EXISTS geocoder_test')

cur.execute('''CREATE TABLE geocoder_test AS SELECT *
            FROM trimet_adds WHERE lat != 0
            LIMIT 10;''') # using LIMIT for testing

cur.execute('''ALTER TABLE geocoder_test
            ADD COLUMN id SERIAL NOT NULL PRIMARY KEY;''')

# adding blank results fields for each geocoder
for aGeocoder in geocoder_dict.keys():
    sqlString = "ALTER TABLE geocoder_test ADD " + aGeocoder \
    + "_address text, ADD " + aGeocoder + "_dist_ft numeric;"
    cur.execute(sqlString)

counter = 0
#cur.execute("SELECT * FROM geocoder_test")
cur.execute("SELECT * FROM geocoder_test WHERE google_dist_ft IS NULL;")
for row in cur.fetchall():
    g = ''
    counter += 1
    if counter % 50 == 0:
        print counter, 'rows done'
    if counter == 6:
        break
    
    anAdd = strip_addresses_of_zip_country(row[0])
    print '\n\n', anAdd, '\n'
    for aGeocoder in geocoder_dict.keys():
        if aGeocoder == 'trimet':
            gc = tm_geocoder.query(anAdd, rows=1, start=0)
            try:
                results = gc.results[0]
                addr = (str.decode('{0}, {1}, OR'.format(results['name'].replace("'", "\''"), results['city'])))
            except:
                addr = 'Null'
                lat = 'Null'
                lng = 'Null'
            print aGeocoder, ':', addr, ", maxScore:", gc.maxScore
        # I have to call mapzen outside of the geocoder wrapper because
        # without the bbox parameters (which I can't pass through geocoder)
        # it was returning some very wacky results - Kansas, Iowa, FL -
        # and just in the first 10 records.
        elif aGeocoder == 'mapzen':
            request = "https://search.mapzen.com/v1/search?api_key=" \
                    + geocoder_dict[aGeocoder] + "&text=" + anAdd + bbox
            resp = requests.get(request, verify=False).text
            result = parse_mapzen_response(resp)
            addr = result['label']
            lat = result['latitude']
            lng = result['longitude']
            print aGeocoder, ':', addr, ", confidence:", result['confidence']
        elif aGeocoder == 'rlis':
            result = rlis_geocode(anAdd, geocoder_dict[aGeocoder])
            if result:
                addr = result['fullAddress']
                lat = result['lat']
                lng = result['lng']
                print aGeocoder, addr, 'score:', result['score']
            else:
                addr = 'Null'
                lat = 'Null'
                lng = 'Null'
        else:
            this_geocoder = getattr(geocoder, aGeocoder)
            if geocoder_dict[aGeocoder] == '':
                g = this_geocoder(anAdd)
                while g.status == 'OVER_QUERY_LIMIT':
                    print ("over", aGeocoder,
                           "query limit - will try again in 30 minutes")
                    time.sleep(1800)
                    g = this_geocoder(anAdd)
            else:
                # another possible parameter to pass for mapbox is
                # proximity=pdxlatlng (makes it favor results near that
                # point) but it didn't improve things for a small sample 
                g = this_geocoder(anAdd, key=geocoder_dict[aGeocoder]) 
            
            print aGeocoder, ':', g.address, ", conf.:", g.confidence, ", qual.:", g.quality

            addr, lat, lng = getAddrAndLatLng(g)
        copyResultsToTable(addr, lat, lng)
    conn.commit()    
conn.close()
