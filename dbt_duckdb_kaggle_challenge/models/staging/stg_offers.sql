with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}

    select * from {{ ref('offers') }}
),

renamed AS (
    SELECT
       offer AS offer_id,
       category AS product_category,
       quantity,
       company AS company_id,
       offervalue,
       brand AS brand_id

    FROM source
)

SELECT * FROM renamed