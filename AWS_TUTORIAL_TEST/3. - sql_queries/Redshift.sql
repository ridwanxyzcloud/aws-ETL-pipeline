-- Create customers table
CREATE TABLE customers (
    customer_id UUID PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    address VARCHAR(255),
    signup_date DATE,
    is_active BOOLEAN,
    is_premium BOOLEAN
)
DISTSTYLE KEY
DISTKEY(customer_id)
SORTKEY(signup_date);

-- Create products table
CREATE TABLE products (
    product_id UUID PRIMARY KEY,
    product_name VARCHAR(255),
    price DECIMAL(10, 2),
    brand VARCHAR(255),
    category VARCHAR(255)
)
DISTSTYLE EVEN
SORTKEY(category, brand);

-- Create sales table
CREATE TABLE sales (
    sale_id UUID PRIMARY KEY,
    customer_id UUID,
    product_id UUID,
    quantity INT,
    sale_date DATE,
    price DECIMAL(10, 2),
    discount DECIMAL(10, 2),
    total_price DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
)
DISTSTYLE KEY
DISTKEY(customer_id)
SORTKEY(sale_date, customer_id, product_id);



-- Redshift uses a different approach for performance optimization, relying on distribution styles and  sort keys to optimize query performance.
-- This replaces the traditional indexing we noramlly would use 