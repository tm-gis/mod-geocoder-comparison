import arcpy

test_suite = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\shp\geocoder_test_suite_taxlot.shp"

with arcpy.da.UpdateCursor(test_suite, ["SITEADDR", "House_Numb", "Prefix", "Street_Nam", "Street_Typ",
                                        "Discrepanc"]) as cursor:
    for row in cursor:
        siteaddr = row[0]
        test_addr = " ".join(i for i in [row[1], row[2], row[3].upper(), row[4].upper()])
        if siteaddr != test_addr:
            row[5] = "Y"
        else:
            row[5] = "N"
        cursor.updateRow(row)
del cursor
