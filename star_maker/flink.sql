CREATE TABLE raw_kafka_stream (
    customer_first_name VARCHAR,
    customer_last_name VARCHAR,
    customer_age INT,
    customer_email VARCHAR,
    customer_country VARCHAR,
    customer_postal_code VARCHAR,
    customer_pet_type VARCHAR,
    customer_pet_name VARCHAR,
    customer_pet_breed VARCHAR,
    seller_first_name VARCHAR,
    seller_last_name VARCHAR,
    seller_email VARCHAR,
    seller_country VARCHAR,
    seller_postal_code VARCHAR,
    product_name VARCHAR,
    product_category VARCHAR,
    product_price DECIMAL,
    product_quantity INT,
    sale_date DATE,
    sale_quantity INT,
    sale_total_price DECIMAL,
    store_name VARCHAR,
    store_location VARCHAR,
    store_city VARCHAR,
    store_state VARCHAR,
    store_country VARCHAR,
    store_phone VARCHAR,
    store_email VARCHAR,
    pet_category VARCHAR,
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
    supplier_name VARCHAR,
    supplier_contact VARCHAR,
    supplier_email VARCHAR,
    supplier_phone VARCHAR,
    supplier_address VARCHAR,
    supplier_city VARCHAR,
    supplier_country VARCHAR
) WITH (
    'connector' = 'kafka',
    'topic' = 'sales_topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink_consumer_group',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = 'latest-offset'
);

CREATE TABLE dim_customers_sink (
    customer_email VARCHAR,
    customer_first_name VARCHAR,
    customer_last_name VARCHAR,
    customer_age INT,
    customer_country VARCHAR,
    customer_postal_code VARCHAR,
    customer_pet_type VARCHAR,
    customer_pet_name VARCHAR,
    customer_pet_breed VARCHAR,
    PRIMARY KEY (customer_email) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/user',
    'table-name' = 'dim_customers',
    'username' = 'user',
    'password' = 'password'
);

CREATE TABLE dim_sellers_sink (
    seller_email VARCHAR,
    seller_first_name VARCHAR,
    seller_last_name VARCHAR,
    seller_country VARCHAR,
    seller_postal_code VARCHAR,
    PRIMARY KEY (seller_email) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/user',
    'table-name' = 'dim_sellers',
    'username' = 'user',
    'password' = 'password'
);

CREATE TABLE dim_stores_sink (
    store_email VARCHAR,
    store_name VARCHAR,
    store_location VARCHAR,
    store_city VARCHAR,
    store_state VARCHAR,
    store_country VARCHAR,
    store_phone VARCHAR,
    PRIMARY KEY (store_email) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/user',
    'table-name' = 'dim_stores',
    'username' = 'user',
    'password' = 'password'
);

CREATE TABLE dim_suppliers_sink (
    supplier_email VARCHAR,
    supplier_name VARCHAR,
    supplier_contact VARCHAR,
    supplier_phone VARCHAR,
    supplier_address VARCHAR,
    supplier_city VARCHAR,
    supplier_country VARCHAR,
    PRIMARY KEY (supplier_email) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/user',
    'table-name' = 'dim_suppliers',
    'username' = 'user',
    'password' = 'password'
);

CREATE TABLE dim_products_sink (
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
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/user',
    'table-name' = 'dim_products',
    'username' = 'user',
    'password' = 'password'
);

CREATE TABLE IF NOT EXISTS fact_sales_sink (
    customer_email VARCHAR,
    seller_email VARCHAR,
    supplier_email VARCHAR,
    store_email VARCHAR,
    product_id VARCHAR,

    sale_date DATE,
    sale_quantity INT,
    sale_total_price DECIMAL,
    pet_category VARCHAR
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/user',
    'table-name' = 'fact_sales',
    'username' = 'user',
    'password' = 'password'
);


EXECUTE STATEMENT SET
BEGIN
    INSERT INTO dim_customers_sink
    SELECT 
        customer_email,
        customer_first_name,
        customer_last_name,
        customer_age,
        customer_country,
        customer_postal_code,
        customer_pet_type,
        customer_pet_name,
        customer_pet_breed
    FROM raw_kafka_stream;

    INSERT INTO dim_sellers_sink
    SELECT 
        seller_email,
        seller_first_name,
        seller_last_name,
        seller_country,
        seller_postal_code
    FROM raw_kafka_stream;

    INSERT INTO dim_stores_sink
    SELECT 
        store_email,
        store_name,
        store_location,
        store_city,
        store_state,
        store_country,
        store_phone
    FROM raw_kafka_stream;

    INSERT INTO dim_suppliers_sink
    SELECT 
        supplier_email,
        supplier_name,
        supplier_contact,
        supplier_phone,
        supplier_address,
        supplier_city,
        supplier_country
    FROM raw_kafka_stream;

    INSERT INTO dim_products_sink
    SELECT 
        MD5(product_name || product_brand || product_color || 
            CAST(product_price AS VARCHAR) || CAST(product_weight AS VARCHAR)) AS product_id,
        product_name,
        product_category,
        product_price,
        product_quantity,
        product_weight,
        product_color,
        product_size,
        product_brand,
        product_material,
        product_description,
        product_rating,
        product_reviews,
        product_release_date,
        product_expiry_date
    FROM raw_kafka_stream;

    INSERT INTO fact_sales_sink
    SELECT 
        customer_email,
        seller_email,
        supplier_email,
        store_email,
        MD5(product_name || product_brand || product_color || 
            CAST(product_price AS VARCHAR) || CAST(product_weight AS VARCHAR)) AS product_id,
        sale_date,
        sale_quantity,
        sale_total_price,
        pet_category
    FROM raw_kafka_stream;
END;
