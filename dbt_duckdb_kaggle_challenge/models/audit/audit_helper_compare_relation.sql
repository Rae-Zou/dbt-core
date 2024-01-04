{# in dbt Develop #}


{% set old_fct_offers_query %}
select
    offer AS offer_id,
    category AS product_category,
    quantity,
    company AS company_id,
    offervalue,
    brand AS brand_id
from {{ref('offers')}}
{% endset %}


{% set new_fct_offers_query %}
select
    offer_id,
    product_category,
    quantity,
    company_id,
    offervalue,
    brand_id
from {{ ref('stg_offers') }}
{% endset %}


{{ audit_helper.compare_queries(
    a_query=old_fct_offers_query,
    b_query=new_fct_offers_query,
    primary_key="offer_id"
) }}