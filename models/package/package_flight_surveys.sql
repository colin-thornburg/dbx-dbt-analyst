SELECT
    a.flight_number,
    b.Carrier_Group,
    b.destination_airport,
    a.surveyidentifier,
    a.surveyresponsebegintimestamp,
    a.response_id,
    a.comment_txt
FROM {{ ref('dbx_data_engineering', 'prep_survey_data_pivot') }} AS a
INNER JOIN {{ ref('flight_ops', 'prep_flight_data') }} AS b
    ON a.flight_number = b.flight_number