Data Engineering Interview Question. Asked in Delloite Consulting for Snowflake/AWS Data Engineer Role. (3 - 7 years of exp)

1. Given the reviews table, write a query to retrieve the average star rating for each product, grouped by month. The output should display the month as a numerical value, product ID, and average star rating rounded to two decimal places. Sort the output first by month and then by product ID.

Question includes numerous conditions that can be confusing, especially when presented orally during an interview.

CREATE TABLE product_reviews (
    month INT,
    product_id INT,
    average_stars DECIMAL(3, 2)
);
INSERT INTO product_reviews (month, product_id, average_stars) VALUES (5, 25255, 4.00);
INSERT INTO product_reviews (month, product_id, average_stars) VALUES (5, 25600, 4.33);
INSERT INTO product_reviews (month, product_id, average_stars) VALUES (6, 12580, 4.50);
INSERT INTO product_reviews (month, product_id, average_stars) VALUES (6, 50001, 3.50);
INSERT INTO product_reviews (month, product_id, average_stars) VALUES (6, 69852, 4.00);
INSERT INTO product_reviews (month, product_id, average_stars) VALUES (7, 11223, 5.00);
INSERT INTO product_reviews (month, product_id, average_stars) VALUES (7, 69852, 2.50);



#Solution for Snowflake: 

SELECT 
 MONTH(submit_date) AS month, -- Extracts the month from submit_date
 product_id,
 ROUND(AVG(stars), 2) AS average_stars
FROM reviews
GROUP BY 
 MONTH(submit_date), product_id 
ORDER BY 
 month, product_id;

Alternatively EXTRACT() and DATE_PART() functions for MySQL and PostgreSQL respectively:

#MySql

SELECT 
 EXTRACT(MONTH FROM submit_date) AS month,
 product_id,
 ROUND(AVG(stars), 2) AS average_stars
FROM reviews
GROUP BY 
 EXTRACT(MONTH FROM submit_date), product_id
ORDER BY 
 month, product_id;


#PostgreSQL

SELECT 
 DATE_PART('month', submit_date) AS month,
 product_id,
 ROUND(AVG(stars), 2) AS average_stars
FROM 
 reviews
GROUP BY 
 DATE_PART('month', submit_date), product_id
ORDER BY 
 month, product_id;
