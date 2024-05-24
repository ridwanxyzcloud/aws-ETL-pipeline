/* Stored Procedures in this data warehouse help automate repetitive tasks, 
encapsulate complex logic, and enhance performance for specific operations
*/



-- Updates Customer Status
-- updates the is_active status of customers based on their recent purchase activity.
-- This procedure updates the is_active column for each customer based on whether they have made a purchase in the last six months.
CREATE OR REPLACE PROCEDURE update_customer_status()
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE customers
    SET is_active = CASE
        WHEN c.customer_id IN (
            SELECT DISTINCT customer_id
            FROM sales
            WHERE sale_date > CURRENT_DATE - INTERVAL '6 months'
        ) THEN TRUE
        ELSE FALSE
    END
    FROM customers c;
END;
$$;


--  Monthly Sales Report
-- generates a monthly sales report and store it in a new table for quick access.
-- This procedure calculates the total sales, unique customers, and total transactions for each month and stores the results in a temporary table.

CREATE OR REPLACE PROCEDURE generate_monthly_sales_report()
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TEMP TABLE monthly_sales_report AS
    SELECT
        DATE_TRUNC('month', sale_date) AS month,
        SUM(total_price) AS total_sales,
        COUNT(DISTINCT customer_id) AS unique_customers,
        COUNT(*) AS total_transactions
    FROM sales
    GROUP BY 1
    ORDER BY 1;
END;
$$;


-- Restock Notification
-- identifies products that need restocking based on recent sales data.
-- This procedure identifies products that have sold more than a specified threshold in the past month and stores the results in a temporary table. It also raises a notice with the count of such products.

CREATE OR REPLACE PROCEDURE restock_notification(threshold INT)
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TEMP TABLE restock_products AS
    SELECT
        p.product_id,
        p.product_name,
        SUM(s.quantity) AS total_sold,
        p.brand,
        p.category
    FROM products p
    JOIN sales s ON p.product_id = s.product_id
    WHERE s.sale_date > CURRENT_DATE - INTERVAL '1 month'
    GROUP BY p.product_id, p.product_name, p.brand, p.category
    HAVING SUM(s.quantity) >= threshold;
    
    -- Example notification logic (could be email, log entry, etc.)
    RAISE NOTICE 'Products needing restock: %', (SELECT COUNT(*) FROM restock_products);
END;
$$;



-- Top Customers Report
-- generates a report of the top N customers by total sales amount.
-- This procedure generates a report of the top N customers based on the total amount they have spent, storing the results in a temporary table.



CREATE OR REPLACE PROCEDURE top_customers_report(N INT)
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TEMP TABLE top_customers AS
    SELECT
        c.customer_id,
        c.name,
        c.email,
        SUM(s.total_price) AS total_spent
    FROM customers c
    JOIN sales s ON c.customer_id = s.customer_id
    GROUP BY c.customer_id, c.name, c.email
    ORDER BY total_spent DESC
    LIMIT N;
END;
$$;

--  Product Sales Summary
-- generates a summary of sales for each product.
-- This procedure generates a summary report of sales for each product, including total quantity sold and total revenue, and stores the results in a temporary table.


CREATE OR REPLACE PROCEDURE product_sales_summary()
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TEMP TABLE product_sales_summary AS
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        SUM(s.quantity) AS total_quantity_sold,
        SUM(s.total_price) AS total_revenue
    FROM products p
    JOIN sales s ON p.product_id = s.product_id
    GROUP BY p.product_id, p.product_name, p.category
    ORDER BY total_revenue DESC;
END;
$$;


/*
The stored procedures in Amazon Redshift DW cane be automated by creating a Lambda function that invokes the procedure and then schedule this function using CloudWatch Events. 
This setup ensures that your stored procedures run automatically at specified intervals without manual intervention.
*/