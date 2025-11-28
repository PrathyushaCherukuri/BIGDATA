create database challenge1;
use challenge1;


CREATE TABLE sales (
  customer_id VARCHAR(1),
  order_date DATE,
  product_id INTEGER
);

INSERT INTO sales
  (customer_id, order_date, product_id)
VALUES
  ('A', '2021-01-01', '1'),
  ('A', '2021-01-01', '2'),
  ('A', '2021-01-07', '2'),
  ('A', '2021-01-10', '3'),
  ('A', '2021-01-11', '3'),
  ('A', '2021-01-11', '3'),
  ('B', '2021-01-01', '2'),
  ('B', '2021-01-02', '2'),
  ('B', '2021-01-04', '1'),
  ('B', '2021-01-11', '1'),
  ('B', '2021-01-16', '3'),
  ('B', '2021-02-01', '3'),
  ('C', '2021-01-01', '3'),
  ('C', '2021-01-01', '3'),
  ('C', '2021-01-07', '3');
 

CREATE TABLE menu (
  product_id INTEGER,
  product_name VARCHAR(5),
  price INTEGER
);

INSERT INTO menu
  (product_id, product_name, price)
VALUES
  ('1', 'sushi', '10'),
  ('2', 'curry', '15'),
  ('3', 'ramen', '12');
  

CREATE TABLE members (
  customer_id VARCHAR(1),
  join_date DATE
);

INSERT INTO members
  (customer_id, join_date)
VALUES
  ('A', '2021-01-07'),
  ('B', '2021-01-09');
  
  
  show tables;
  
-- 1. What is the total amount each customer spent at the restaurant?
SELECT 
	s.customer_id, 
	sum(price) AS Total_Amount
FROM sales s
JOIN menu m
ON s.product_id = m.product_id
GROUP BY customer_id;

-- 2. How many days has each customer visited the restaurant?
SELECT 
    customer_id,
    COUNT(DISTINCT order_date) AS visit_days
FROM sales
GROUP BY customer_id;

-- 3. What was the first item from the menu purchased by each customer?
WITH ranked AS (
    SELECT
        s.customer_id,
        m.product_name,
        ROW_NUMBER() OVER (
            PARTITION BY s.customer_id
            ORDER BY s.order_date, s.product_id
        ) AS rn
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
)
SELECT customer_id, product_name
FROM ranked
WHERE rn = 1;


-- 4. What is the most purchased item on the menu and how many times was it purchased by all customers?

SELECT 
    m.product_name,
    COUNT(*) AS total_purchased
FROM sales s
JOIN menu m
    ON s.product_id = m.product_id
GROUP BY m.product_name
ORDER BY total_purchased DESC
LIMIT 1;

-- 5. Which item was the most popular for each customer?

WITH ranked_orders AS (
 SELECT
   s.customer_id,
   m.product_name,
   COUNT(*) AS total_ordered,
   RANK() OVER (PARTITION BY s.customer_id ORDER BY COUNT(*) DESC) AS rnk
 FROM sales s
 JOIN menu m ON s.product_id = m.product_id
 GROUP BY s.customer_id, m.product_name
)
SELECT customer_id, product_name, total_ordered
FROM ranked_orders
WHERE rnk = 1
ORDER BY customer_id; 


-- 6. Which item was purchased first by the customer after they became a member?

SELECT 
	s.customer_id, 
    m.product_name AS FirstProduct,
	s.order_date
FROM sales s
JOIN menu m
ON s.product_id = m.product_id
JOIN members mem
ON s.customer_id = mem.customer_id
WHERE s.order_date >= mem.join_date
  AND s.order_date = (
      SELECT MIN(s2.order_date)
      FROM sales s2
      WHERE s2.customer_id = s.customer_id
        AND s2.order_date >= mem.join_date
  )
ORDER BY s.customer_id;

-- 7. Which item was purchased just before the customer became a member?
SELECT 
	s.customer_id, 
    m.product_name AS ProductBeforeMem,
	s.order_date
FROM sales s
JOIN menu m
ON s.product_id = m.product_id
JOIN members mem
ON s.customer_id = mem.customer_id
WHERE s.order_date < mem.join_date
  AND s.order_date = (
      SELECT MAX(s2.order_date)
      FROM sales s2
      WHERE s2.customer_id = s.customer_id
        AND s2.order_date < mem.join_date
  )
ORDER BY s.customer_id;

-- 8. What is the total items and amount spent for each member before they became a member?

SELECT 
    mem.customer_id,
    COUNT(s.product_id) AS total_items,
    SUM(m.price) AS amount_spent
FROM members mem
LEFT JOIN sales s
    ON mem.customer_id = s.customer_id
    AND s.order_date < mem.join_date
LEFT JOIN menu m
    ON s.product_id = m.product_id
GROUP BY mem.customer_id
ORDER BY mem.customer_id;

-- 9.  If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?
SELECT
    s.customer_id,
    SUM(m.price * 10 * IF(m.product_name = 'sushi', 2, 1)) AS total_points
FROM sales s
JOIN menu m
    ON s.product_id = m.product_id
GROUP BY s.customer_id
ORDER BY s.customer_id;


/* 10. In the first week after a customer joins the program (including their join date) they earn 
2x points on all items, not just sushi - how many points do customer A and B have at the end of 
January? */

SELECT 
    s.customer_id,
    SUM(
        m.price * 10
        * CASE 
            WHEN s.order_date BETWEEN mem.join_date AND DATE_ADD(mem.join_date, INTERVAL 6 DAY)
                THEN 2        -- double points in first week
            WHEN m.product_name = 'sushi' 
                THEN 2        -- sushi multiplier outside first week
            ELSE 1
        END
    ) AS total_points
FROM sales s
JOIN menu m
    ON s.product_id = m.product_id
JOIN members mem
    ON s.customer_id = mem.customer_id
WHERE s.order_date <= '2021-01-31'
GROUP BY s.customer_id
ORDER BY s.customer_id;

-- 11.  Write a query using a CTE to find the top 5 customers who have made the highest total payment amount.

WITH total_payments AS (
    SELECT 
        s.customer_id,
        SUM(m.price) AS total_amount
    FROM sales s
    JOIN menu m
        ON s.product_id = m.product_id
    GROUP BY s.customer_id
)
SELECT 
    customer_id,
    total_amount
FROM total_payments
ORDER BY total_amount DESC
LIMIT 5;

	

