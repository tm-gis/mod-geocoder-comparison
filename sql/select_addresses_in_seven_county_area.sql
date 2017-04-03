CREATE TABLE trimet_adds AS
SELECT
  o.address AS address, 
  o.lat AS lat,
  o.lon AS lon,
  o.geom AS geom
FROM oregon as o
JOIN seven_counties AS c
ON ST_contains(c.geom, o.geom);


SELECT * FROM geocoder_test as g, metro_bnd AS m WHERE ST_Contains(m.geom, g.geom);