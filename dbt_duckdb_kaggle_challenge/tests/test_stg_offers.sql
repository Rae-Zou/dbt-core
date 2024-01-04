-- to test column offer_id: it should NOT be smaller than 0
{{
    config(
    severity = 'warn',
    )
}}


SELECT
    offervalue
from {{ ref('stg_offers') }}
where offervalue < 0