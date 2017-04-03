# install Frank's geocoding package with the following command:
# pip install git+https://github.com/OpenTransitTools/utils.git#egg=ott.utils-0.1.0
# pip install git+https://github.com/OpenTransitTools/geocoder.git#egg=ott.geocoder-0.1.0

from ott.geocoder.geosolr import GeoSolr

geocoder_url = 'http://maps.trimet.org/solr'
geocoder = GeoSolr(geocoder_url)

search = '4012 SE 17th Ave'
gc = geocoder.query(search, rows=1, start=0)
results = gc.results[0]

print 'Coordinate Info:'
print '{0}, {1}'.format(results['lon'], results['lat'])
print '{0}, {1}'.format(results['x'], results['y'])
print '\nAddress Info:'
print '{0}, {1}'.format(results['name'], results['city'])