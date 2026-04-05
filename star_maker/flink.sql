CREATE TABLE raw_kafka_stream (
    id INT,
    sale_customer_id INT,
    sale_seller_id INT,
    sale_product_id INT,
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
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/user',
    'table-name' = 'dim_customers',
    'username' = 'user',
    'password' = 'password'
);

CREATE TABLE dim_sellers_sink (
    id INT,
    email VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    country VARCHAR,
    postal_code VARCHAR,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/user',
    'table-name' = 'dim_sellers',
    'username' = 'user',
    'password' = 'password'
);

CREATE TABLE dim_stores_sink (
    email VARCHAR,
    name VARCHAR,
    location VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    phone VARCHAR,
    PRIMARY KEY (email) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/user',
    'table-name' = 'dim_stores',
    'username' = 'user',
    'password' = 'password'
);

CREATE TABLE dim_suppliers_sink (
    email VARCHAR,
    name VARCHAR,
    contact VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    country VARCHAR,
    PRIMARY KEY (email) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/user',
    'table-name' = 'dim_suppliers',
    'username' = 'user',
    'password' = 'password'
);

CREATE TABLE dim_products_sink (
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
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/user',
    'table-name' = 'dim_products',
    'username' = 'user',
    'password' = 'password'
);

CREATE TABLE IF NOT EXISTS fact_sales_sink (
    id INT,
    customer_id INT,
    seller_id INT,
    product_id INT,
    supplier_email VARCHAR,
    store_email VARCHAR,

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
        sale_customer_id AS id,
        customer_email AS email,
        customer_first_name as first_name,
        customer_last_name as last_name,
        customer_age as age,
        customer_country as country,
        customer_postal_code as postal_code,
        customer_pet_type as pet_type,
        customer_pet_name as pet_name,
        customer_pet_breed as pet_breed
    FROM raw_kafka_stream;

    INSERT INTO dim_sellers_sink
    SELECT 
        sale_seller_id AS id,
        seller_email as email,
        seller_first_name as first_name,
        seller_last_name as last_name,
        seller_country as country,
        seller_postal_code as postal_code
    FROM raw_kafka_stream;

    INSERT INTO dim_stores_sink
    SELECT 
        store_email as email,
        store_name as name,
        store_location as location,
        store_city as city,
        store_state as state,
        store_country as country,
        store_phone as phone
    FROM raw_kafka_stream;

    INSERT INTO dim_suppliers_sink
    SELECT 
        supplier_email as email,
        supplier_name as name,
        supplier_contact as contact,
        supplier_phone as phone,
        supplier_address as address,
        supplier_city as city,
        supplier_country as country
    FROM raw_kafka_stream;

    INSERT INTO dim_products_sink
    SELECT 
        sale_product_id as id,
        product_name as name,
        product_category as category,
        product_price as price,
        product_quantity as quantity,
        product_weight as weight,
        product_color as color,
        product_size as size,
        product_brand as brand,
        product_material as material,
        product_description as description,
        product_rating as rating,
        product_reviews as reviews,
        product_release_date as release_date,
        product_expiry_date as expiry_date
    FROM raw_kafka_stream;

    INSERT INTO fact_sales_sink
    SELECT 
        id,
        sale_customer_id as customer_id,
        sale_seller_id as seller_id,
        sale_product_id as product_id,
        supplier_email,
        store_email,
        sale_date,
        sale_quantity,
        sale_total_price,
        pet_category
    FROM raw_kafka_stream;
END;
