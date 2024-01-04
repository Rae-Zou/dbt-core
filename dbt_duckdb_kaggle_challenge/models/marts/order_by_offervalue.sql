-- This overrides the config in dbt_project.yml and this model will not require tests
--{{config(required_tests = None)}}

SELECT 
    product_category
    , company_id
    , brand_id
    , quantity
    , offervalue
    , offerquantity
FROM {{ ref('int_offers') }}
ORDER BY offervalue DESC