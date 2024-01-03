WITH offers AS 
     (
     SELECT product_category
            , company_id
            , brand_id
            , min(quantity) AS quantity
            , MIN(offervalue) AS offervalue
            , COUNT(offer_id) offerquantity
       FROM {{ ref('stg_offers') }}
       GROUP BY 1, 2, 3
     )

SELECT *
  FROM offers