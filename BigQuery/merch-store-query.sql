-- CREATE SNAPSHOT DATE TO WORK WITH
DECLARE snapshot_date TIMESTAMP DEFAULT TIMESTAMP "2023-09-19 00:00:00";

--CREATE PRODUCTS VIEW 
CREATE OR REPLACE VIEW merch-store-399816.products.view_merged_products AS(
WITH Merged_products AS(
  SELECT*FROM merch-store-399816.electronics_product.electronics_product_table
  UNION ALL
  SELECT*FROM merch-store-399816.fashion_product.fashion_product_table 
)
SELECT*FROM Merged_products);

--CREATE PURCHASE VIEW
CREATE OR REPLACE VIEW merch-store-399816.purchase.view_merged_purchases AS(
WITH Merged_purchases AS(
  SELECT*FROM merch-store-399816.electronics_purchase.electronics_purchase_table
  UNION ALL
  SELECT*FROM merch-store-399816.fasion_purchase.fashion_purchase_table
)
SELECT*FROM Merged_purchases);

--CREATE CUSTOMERS VIEW
CREATE OR REPLACE VIEW merch-store-399816.customers.view_merged_customers AS (
  WITH Merged_customers AS (
    SELECT 
      PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', purchase_date) AS purchase_date,
      order_id,
      product_id,
      vmp.customer_id,
      age,
      gender,
      email,
      country,
      Total_amount,
      CASE
        WHEN age < 40 THEN 'Young'
        WHEN age >= 40 AND age < 65 THEN 'Middle-aged'
        ELSE 'Elderly'
      END AS AgeGroup
    FROM
      merch-store-399816.purchase.view_merged_purchases vmp
    LEFT JOIN
      merch-store-399816.customers.customers_table c ON vmp.customer_id = c.customer_id
  )
  SELECT * FROM Merged_customers
);


--CREATE DATA MARTS

--Create Marketing Mart

CREATE OR REPLACE VIEW merch-store-399816.marketing.Marketing_mart AS (
--RFM CTE. To create the frequency, recency and monetary value of each customer 
WITH RFM AS(
  SELECT customer_id,
  AgeGroup,
  email,
  country,
  Total_amount AS MonetaryValue,
  COUNT(order_id) AS Frequency,
  MAX(purchase_date) AS LastOrderDate,
  TIMESTAMP '2023-09-19 00:00:00' AS SnapshotDate, -- Use the constant value directly to prevent error
  DATE_DIFF(TIMESTAMP '2023-09-19 00:00:00', MAX(purchase_date), DAY) AS Recency,
  FROM merch-store-399816.customers.view_merged_customers
  GROUP BY customer_id,AgeGroup,email,country,Total_amount 
),
-- Create quartile values of range 1-5 for each recency, frequency, and monetary value.
 Quartile_grouping AS (
   SELECT R.*,
   NTILE(5) OVER(ORDER BY Recency DESC) AS RFM_Recency,
   NTILE(5) OVER(ORDER BY Frequency) AS RFM_Frequency,
   NTILE(5) OVER(ORDER BY MonetaryValue) AS RFM_Monetaryvalue,
 FROM RFM R
 ),
--Concatenate RFM_Recency, RFM_Frequency, RFM_MonetaryValue
 RFM_Concat AS (
   SELECT Q.*,
   CONCAT(CAST(RFM_Recency AS STRING),CAST(RFM_Frequency AS STRING),CAST(RFM_MonetaryValue AS STRING)) AS RFM_Score
 FROM Quartile_grouping Q
 ),
--Assign customer segment name 
 Customer_segmentation AS (
   SELECT RC.*,
   CASE 
     WHEN  RFM_Score IN ('555', '554', '544', '545', '454', '455','445') THEN 'Champion'
     WHEN RFM_Score IN ('543', '444', '435', '355', '354', '345', '344', '335') THEN 'Loyal Customer'
     WHEN RFM_Score IN ('553', '551', '552', '541', '542', '533', '532', '531',
                       '452', '451', '442', '441', '431', '453', '433', '432',
                        '423', '353', '352', '351', '342', '341', '333', '323') THEN 'Potential Loyalist'
      WHEN RFM_Score IN ('512', '511', '422', '421', '412', '411', '311') THEN 'New Customer'
      WHEN RFM_Score IN ('525', '524', '523', '522', '521', '515', '514', '513',
                          '425', '424', '413', '414', '415', '315', '314', '313') THEN 'Promising'
      WHEN RFM_Score IN ('535', '534', '443', '434', '343','334', '325', '324') THEN 'Needs Attention'
      WHEN RFM_Score IN  ('331', '321', '312', '221', '213', '231', '241', '251') THEN 'About To Sleep'
      WHEN RFM_Score IN ('255', '254', '245', '244', '253', '252', '243', '242',
                          '235', '234', '225', '224', '153', '152', '145', '143',
                          '142', '135', '134', '133', '125', '124') THEN 'At Risk'
      WHEN RFM_Score IN ('155', '154', '144', '214', '215', '115', '114', '113') THEN 'Cannot Lose'
      WHEN RFM_Score IN ('332', '322', '233', '232', '223', '222', '132', '123',
                          '122', '212', '211') THEN 'Hibernating'
      WHEN  RFM_Score IN ('111', '112', '121', '131', '141', '151') THEN 'Lost Customer'
      ELSE 'No Defined Segment'
      END AS Customer_Segments,
     FROM RFM_Concat RC
 )
 SELECT*FROM Customer_segmentation);

--Create Sales Mart
CREATE OR REPLACE VIEW merch-store-399816.sales.Sales_mart AS (
  WITH sales_mt AS (
    SELECT 
      mp.order_id,
      PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', purchase_date) AS purchase_date,
      mp.customer_id,
      mp.product_id,
      mp.selling_price,
      mp.quantity,
      mp.Total_amount,
      ct.country
    FROM merch-store-399816.purchase.view_merged_purchases mp
    LEFT JOIN merch-store-399816.customers.customers_table ct ON mp.customer_id = ct.customer_id
  )
  SELECT * FROM sales_mt
);
  

