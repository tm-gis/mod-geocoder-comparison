import csv
import geocoder
from geocoder_comp_2017 import getAddrAndLatLng
import time

in_csv = r"G:\PUBLIC\LinT\Geocoder_Testing\geocoder_test_suite_data.csv"
out_csv = r"G:\PUBLIC\LinT\Geocoder_Testing\geocoder_test_suite_data_with_coordinates_url.csv"
location_dict = {}  # location_dict["Type"]["Name"] = ["Street", "X_Coord", "Y_Coord", "Google Maps link"]

with open(out_csv, "wb") as out_file:
    writer = csv.writer(out_file, delimiter=",")
    with open(in_csv, "rb") as csv_file:
        reader = csv.reader(csv_file, delimiter=",")
        legend = ['Type', 'Source', 'Verified', 'Name', 'Prefix', 'House_Number', 'Unit_Type', 'Unit_Number',
                  'Street_Name','Street_Type', 'City', 'State', 'Zip', 'Stop_ID', 'X_Coord', 'Y_Coord', 'Lat', 'Lon']
        legend += ["Google_Maps_URL"]
        writer.writerow(legend)

        for r in reader:
            if r[0] != "Type":
                Type = r[legend.index('Type')]
                Source = r[legend.index('Source')]
                Verified = r[legend.index('Verified')]
                Name = r[legend.index('Name')]
                Prefix = r[legend.index('Prefix')]
                House_Number = r[legend.index('House_Number')]
                Unit_Type = r[legend.index('Unit_Type')]
                Unit_Number = r[legend.index('Unit_Number')]
                Street_Name = r[legend.index('Street_Name')]
                Street_Type = r[legend.index('Street_Type')]
                City = r[legend.index('City')]
                State = r[legend.index('State')]
                Zip = r[legend.index('Zip')]
                Stop_ID = r[legend.index('Stop_ID')]
                X_Coord = r[legend.index('X_Coord')]
                Y_Coord = r[legend.index('Y_Coord')]
                Lat = r[legend.index('Lat')]
                Lon = r[legend.index('Lon')]

                this_geocoder = getattr(geocoder, 'google')
                current = Name + ", Portland, Oregon"
                g = this_geocoder(current)
                while g.status == 'OVER_QUERY_LIMIT':
                    print ("over", 'google',
                           "query limit - will try again in 30 minutes")
                    time.sleep(1800)
                    g = this_geocoder(current)
                addr, lat, lng = getAddrAndLatLng(g)

                if Type not in location_dict:
                    location_dict[Type] = {}
                if Name not in location_dict[Type]:
                    base_url = "https://www.google.com/maps/@" + str(lat) + "," + str(lng) + "," + "16.75z"
                    location_dict[Type][Name] = [addr, lng, lat, base_url]

                address_string = addr.replace(", ", ",")
                address_list_comma = address_string.split(",")

                if Type != "Intersections":
                    try:
                        House_Number = address_list_comma[0].split(" ")[0]
                        Street_Name = " ".join(a for a in address_list_comma[0].split(" ")[1: len(address_list_comma[0].split(" ")) - 1])
                        if Street_Name.split(" ")[0] in ["NE", "NW", "SE", "SW", "E", "W", "N", "S"]:
                            Prefix = Street_Name.split(" ")[0]
                            Street_Name = " ".join(s for s in Street_Name.split(" ")[1:])
                        Street_Type = address_list_comma[0].split(" ")[len(address_list_comma[0].split(" ")) - 1]
                        City = address_list_comma[1]
                        State = address_list_comma[2].split(" ")[0]
                        Zip = address_list_comma[2].split(" ")[1]
                    except:
                        House_Number = r[legend.index('House_Number')]
                        Unit_Type = r[legend.index('Unit_Type')]
                        Unit_Number = r[legend.index('Unit_Number')]
                        Street_Name = addr
                        Street_Type = r[legend.index('Street_Type')]
                        City = r[legend.index('City')]
                        State = r[legend.index('State')]
                        Zip = r[legend.index('Zip')]
                else:
                    Street_Name = addr

                row = [Type, Source, Verified, Name, Prefix, House_Number, Unit_Type, Unit_Number,
                       Street_Name, Street_Type, City, State, Zip, Stop_ID, X_Coord, Y_Coord,
                       location_dict[Type][Name][2], location_dict[Type][Name][1], location_dict[Type][Name][3]]
                writer.writerow(row)
del csv_file
del out_file
