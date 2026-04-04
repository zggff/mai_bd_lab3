CREATE TABLE IF NOT EXISTS dim_customers (
    customer_email VARCHAR,
    customer_first_name VARCHAR,
    customer_last_name VARCHAR,
    customer_age INT,
    customer_country VARCHAR,
    customer_postal_code VARCHAR,
    customer_pet_type VARCHAR,
    customer_pet_name VARCHAR,
    customer_pet_breed VARCHAR,
    PRIMARY KEY (customer_email)
);

CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_email VARCHAR,
    seller_first_name VARCHAR,
    seller_last_name VARCHAR,
    seller_country VARCHAR,
    seller_postal_code VARCHAR,
    PRIMARY KEY (seller_email)
);


CREATE TABLE IF NOT EXISTS dim_stores (
    store_email VARCHAR,
    store_name VARCHAR,
    store_location VARCHAR,
    store_city VARCHAR,
    store_state VARCHAR,
    store_country VARCHAR,
    store_phone VARCHAR,
    PRIMARY KEY (store_email)
);

CREATE TABLE IF NOT EXISTS dim_suppliers (
    supplier_email VARCHAR,
    supplier_name VARCHAR,
    supplier_contact VARCHAR,
    supplier_phone VARCHAR,
    supplier_address VARCHAR,
    supplier_city VARCHAR,
    supplier_country VARCHAR,
    PRIMARY KEY (supplier_email)
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR,
    product_name VARCHAR,
    product_category VARCHAR,
    product_price DECIMAL,
    product_quantity INT,
    product_weight DECIMAL,
    product_color VARCHAR,
    product_size VARCHAR,
    product_brand VARCHAR,
    product_material VARCHAR,
    product_description VARCHAR,
    product_rating DECIMAL,
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE,
    PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    id SERIAL PRIMARY KEY,
    customer_email VARCHAR REFERENCES dim_customers(customer_email),
    seller_email VARCHAR REFERENCES dim_sellers(seller_email),
    supplier_email VARCHAR REFERENCES dim_suppliers(supplier_email),
    store_email VARCHAR REFERENCES dim_stores(store_email),
    product_id VARCHAR REFERENCES dim_products(product_id),

    sale_date DATE,
    sale_quantity INT,
    sale_total_price DECIMAL,
    pet_category VARCHAR
);
