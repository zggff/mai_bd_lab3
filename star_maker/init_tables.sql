CREATE TABLE IF NOT EXISTS dim_customers (
    id INT,
    email VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    age INT,
    country VARCHAR,
    postal_code VARCHAR,
    pet_type VARCHAR,
    pet_name VARCHAR,
    pet_breed VARCHAR,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dim_sellers (
    id INT,
    email VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    country VARCHAR,
    postal_code VARCHAR,
    PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS dim_stores (
    email VARCHAR,
    name VARCHAR,
    location VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    phone VARCHAR,
    PRIMARY KEY (email)
);

CREATE TABLE IF NOT EXISTS dim_suppliers (
    email VARCHAR,
    name VARCHAR,
    contact VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    country VARCHAR,
    PRIMARY KEY (email)
);

CREATE TABLE IF NOT EXISTS dim_products (
    id INT,
    name VARCHAR,
    category VARCHAR,
    price DECIMAL,
    quantity INT,
    weight DECIMAL,
    color VARCHAR,
    size VARCHAR,
    brand VARCHAR,
    material VARCHAR,
    description VARCHAR,
    rating DECIMAL,
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    id INT PRIMARY KEY,
    customer_id INT REFERENCES dim_customers(id),
    seller_id INT REFERENCES dim_sellers(id),
    product_id INT REFERENCES dim_products(id),
    supplier_email VARCHAR REFERENCES dim_suppliers(email),
    store_email VARCHAR REFERENCES dim_stores(email),

    sale_date DATE,
    sale_quantity INT,
    sale_total_price DECIMAL,
    pet_category VARCHAR
);
