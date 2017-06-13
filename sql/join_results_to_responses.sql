select * from geocoder_responses
left join results_without_google
on geocoder_responses.location_id = results_without_google.location_id
and geocoder_responses.geocoder = results_without_google.geocoder
order by geocoder_responses.location_id;
