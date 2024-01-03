
/*
    Defines a final analytical flat table with transactions data by yearmonth
*/

SELECT transaction_id
       , product_category
       , company_id
       , brand_id
       , STRFTIME('%Y%m', date) AS yearmonth
       , purchasetype
       , offered
       , discounted
       , SUM(purchaseamount) AS purchaseamount
       , SUM(purchasequantity) AS purchasequantity
  FROM {{ ref('int_transactions_offers') }}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8