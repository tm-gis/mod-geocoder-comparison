select * from geocoder_responses
left join results
on geocoder_responses.location_id = results.location_id
and geocoder_responses.geocoder = results.geocoder
order by geocoder_responses.location_id;
