# Data dictionary

## orders.csv
- order_id: order-line/order identifier used for this exercise
- order_date, ship_date: ISO dates
- customer_id, product_id: join keys
- quantity: units sold
- discount: fraction from 0 to 1
- sales, profit: order-line financial values
- ship_mode: shipping service

## customers.csv
- customer_id: primary key
- segment: customer segment
- region, city: customer geography

## products.csv
- product_id: primary key
- product_name, category: product classification
- list_price: list price per unit
- unit_cost: cost per unit

## returns.csv
- order_id: returned order identifier
- return_reason: reason for return

The data is synthetic. Amounts are illustrative and do not represent a real company.
