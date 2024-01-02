WITH offers AS 
     (
      SELECT 
        *
        , row_number () over (partition by brand_id, product_category, company_id 
                                  ORDER BY offer_id DESC) AS mostrecent 
                                  -- mark the most recent row for offers when duplicated
        FROM {{ ref('stg_offers') }}
     )
     ,transactions AS 
     (
      SELECT 
        *
        , case when purchaseamount < 0 then 'RETURN'
                  else 'PURCHASE'
              end purchasetype -- add purchases type
        FROM {{ ref('stg_transactions') }}
     )
     , source_data AS 
     (
     SELECT trans.transaction_id
            , trans.dept
            , trans.chain
            , trans.product_category
            , trans.company_id
            , trans.brand_id
            , trans.date
            , trans.productsize
            , trans.productmeasure
            , trans.purchasequantity
            , trans.purchaseamount
            , trans.purchasetype
            , CASE WHEN purchaseamount > 0
                        AND offers.quantity IS NOT NULL THEN 'Yes'
                   ELSE 'No'
              END offered
            , CASE WHEN purchasequantity >= offers.quantity
                        AND purchaseamount > 0
                        THEN 'Yes'
                   ELSE 'No'
              END discounted
       FROM transactions AS trans
            LEFT JOIN offers
                   ON trans.product_category = offers.product_category 
                  AND trans.brand_id = offers.brand_id
                  AND trans.company_id = offers.company_id
            
        WHERE offers.mostrecent = 1
     )

SELECT *
  FROM source_data