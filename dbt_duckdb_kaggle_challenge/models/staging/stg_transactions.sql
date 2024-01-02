with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}

    select * from {{ ref('transactions') }}
),

renamed AS (
    SELECT
       id AS transaction_id,
       chain,
       dept,
       category AS product_category,
       company AS company_id,
       brand AS brand_id,
       date,
       productsize,
       productmeasure,
       purchasequantity,
       purchaseamount

    FROM source
)

SELECT * FROM renamed