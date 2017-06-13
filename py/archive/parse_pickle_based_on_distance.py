import py.pickle
import sys
import csv
import os

sys.path.append(r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\py")

from py.compare_geocoders_using_test_suite import haversine, get_test_suite, get_geocoder_input

geocoder_responses = py.pickle.load(open(r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\results\052317\responses.p", "rb"))
spreadsheet = "1b0zxcb_5w0M6ydStkVlL9ceIAs5P_gJhdQNLfhd0pyA"
spreadsheet_range = 'Locations!A1:Q'

test_suite_legend, test_suite_data_list = get_test_suite(spreadsheet, spreadsheet_range)

results_dict = {}
type_source_total = {}


def compare_geocoders_using_distance(distance_feet):
    for t in test_suite_data_list:
        test_suite_type = t[test_suite_legend.index("Type")]
        source = t[test_suite_legend.index("Source")]
        name = t[test_suite_legend.index("Name")]
        prefix = t[test_suite_legend.index("Prefix")]
        house_number = t[test_suite_legend.index("House_Number")]
        unit_type = test_suite_legend.index("Unit_Type")
        unit_number = t[test_suite_legend.index("Unit_Number")]
        street_name = t[test_suite_legend.index("Street_Name")]
        street_type = t[test_suite_legend.index("Street_Type")]
        city = t[test_suite_legend.index("City")]
        state = t[test_suite_legend.index("State")]
        test_suite_zip = t[test_suite_legend.index("Zip")]
        stop_id = t[test_suite_legend.index("Stop_ID")]
        x_coord = t[test_suite_legend.index("X_Coord")]
        y_coord = t[test_suite_legend.index("Y_Coord")]
        test_suite_lat = float(t[test_suite_legend.index("Lat")])
        test_suite_lon = float(t[test_suite_legend.index("Lon")])

        geocoder_input = get_geocoder_input(t, test_suite_legend)

        if geocoder_input in geocoder_responses:
            for g in geocoder_responses[geocoder_input]:
                if g not in results_dict:
                    results_dict[g] = {}
                if (test_suite_type, source) not in results_dict[g]:
                    results_dict[g][(test_suite_type, source)] = 0
                if (test_suite_type, source) not in type_source_total:
                    type_source_total[(test_suite_type, source)] = 0
                geocoder_lat = geocoder_responses[geocoder_input][g][0]
                geocoder_long = geocoder_responses[geocoder_input][g][1]
                if haversine(test_suite_lon, test_suite_lat, geocoder_long, geocoder_lat) <= distance_feet:
                    results_dict[g][(test_suite_type, source)] += 1
                if g == "trimet":
                    if test_suite_type != "POIs":
                        type_source_total[(test_suite_type, source)] += 1
                    else:
                        if ("POIs", "Landmarks") not in type_source_total:
                            type_source_total[("POIs", "Landmarks")] = 0
                        type_source_total[("POIs", "Landmarks")] += 1

    poi_dict = {}
    results_file = os.path.join(r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\csv\distance_comparison_052317",
                                str(distance_feet) + ".txt")
    with open(results_file, "wb") as text_file:
        writer = csv.writer(text_file, delimiter="\t")
        legend = ["Geocoder", "Type", "Source", "Correctly Geocoded", "Total Addresses", "PERCENTAGE CORRECT"]
        writer.writerow(legend)
        for r in results_dict:
            poi_dict[r] = {}
            for (t, s) in results_dict[r]:
                if t != "POIs":
                    row = [r, t, s, results_dict[r][(t, s)], type_source_total[(t, s)],
                           float(results_dict[r][(t, s)]) / float(type_source_total[(t, s)])]
                    writer.writerow(row)
                else:
                    if ("POIs", "Landmarks") not in poi_dict[r]:
                        poi_dict[r][("POIs", "Landmarks")] = 0
                    poi_dict[r][("POIs", "Landmarks")] += results_dict[r][(t, s)]
        for p in poi_dict:
            row = [p, "POIs", "Landmarks", poi_dict[p][("POIs", "Landmarks")], type_source_total[("POIs", "Landmarks")],
                   float(poi_dict[p][("POIs", "Landmarks")]) / float(type_source_total[("POIs", "Landmarks")])]
            writer.writerow(row)
    del text_file
    return

for i in range(25, 501, 1):
    compare_geocoders_using_distance(i)
