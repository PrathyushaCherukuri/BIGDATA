CREATE DATABASE shopping_cart;
USE shopping_cart;

CREATE TABLE items (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    qty INT,
    cost DOUBLE
);

INSERT INTO items VALUES
(1,'Biscuits',5,20.5),
(2,'Cereals',10,90),
(3,'Chicken',20,100);

CREATE TABLE customers (
    cust_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50),
    address VARCHAR(200),
    distance INT
);

CREATE TABLE orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    cust_id INT,
    total_items_cost DOUBLE,
    delivery_charge DOUBLE,
    final_amount DOUBLE,
    FOREIGN KEY (cust_id) REFERENCES customers(cust_id)
);

CREATE TABLE order_items (
    oi_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    item_name VARCHAR(50),
    qty INT,
    cost DOUBLE,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
