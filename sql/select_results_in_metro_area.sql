SELECT g.*
FROM geocoder_test as g
JOIN metro_bnd AS m
ON ST_contains(m.geom, g.geom);
