-- ############   task 1   ################# --
SELECT * FROM orders_table

-- Alter the data type of date_uuid column to UUID
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

-- Alter the data type of user_uuid column to UUID
ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

-- Alter the data type of card_number column to VARCHAR with maximum length
SELECT MAX(LENGTH(CAST(card_number AS VARCHAR))) AS max_card_number_length FROM orders_table;
SELECT MAX(LENGTH(CAST(store_code AS VARCHAR))) AS max_store_code_length FROM orders_table;
SELECT MAX(LENGTH(CAST(product_code AS VARCHAR))) AS max_product_code_length FROM orders_table;



ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(22);

-- Alter the data type of store_code column to VARCHAR with maximum length 
ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(12);

-- Alter the data type of product_code column to VARCHAR with maximum length 
ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(11);

-- Alter the data type of product_quantity column to SMALLINT
ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT;


-- ############   task 2   ################# --
SELECT * FROM dim_users

-- Alter first name
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255)

-- Alter last name
ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR(255)

--Alter date_of_birth
ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE

--Alter country_code
SELECT MAX(LENGTH(CAST(country_code AS VARCHAR))) FROM dim_users

ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(3)

-- Alter the data type of user_uuid column to UUID
ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

--Alter join_date
ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE

-- SELECT * FROM dim_users
-- WHERE join_date NOT LIKE '%-__'


-- ############   task 3  ################# --
SELECT * FROM dim_store_details

-- Alter 
-- Step 1: Create a new column with the desired data type
ALTER TABLE dim_store_details
ADD COLUMN longitude_new double precision;
-- Step 2: Update the new column with the values from the existing column
UPDATE dim_store_details
SET longitude_new = CASE WHEN longitude IS NULL THEN NULL ELSE CAST(longitude AS double precision) END;
-- Step 3: Drop the old column
ALTER TABLE dim_store_details
DROP COLUMN longitude;
-- Step 4: Rename the new column to the original column name
ALTER TABLE dim_store_details
RENAME COLUMN longitude_new TO longitude;


ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT

-- Alter locality
ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255);

--Alter store_code
SELECT MAX(LENGTH(CAST(store_code AS VARCHAR))) FROM dim_store_details

ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(12)

-- Alter staffnum
ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT USING (staff_numbers::smallint);

-- Alter openning_date
ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255) 




-- Step 1: Create a new column with the desired data type
ALTER TABLE dim_store_details
ADD COLUMN latitude_new double precision;
-- Step 2: Update the new column with the values from the existing column
UPDATE dim_store_details
SET latitude_new = CASE WHEN latitude IS NULL THEN NULL ELSE CAST(latitude AS double precision) END;
-- Step 3: Drop the old column
ALTER TABLE dim_store_details
DROP COLUMN latitude;
-- Step 4: Rename the new column to the original column name
ALTER TABLE dim_store_details
RENAME COLUMN latitude_new TO latitude;

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE double precision USING (latitude::double precision);

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT

ALTER TABLE dim_store_details
ALTER COLUMN  country_code TYPE VARCHAR(2)

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255)


-- ############   task 4  ################# --

--The product_price column has a £ character which you need to remove using SQL.
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

-- Add a new column weight_class which will contain human-readable values based on the weight range of the product.
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE numeric USING (product_price::numeric);

-- Add the new column
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(255);

-- Update the new column based on the weight column
UPDATE dim_products
SET weight_class = 
    CASE 
        WHEN weight < 2 THEN 'LIGHT'
        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
        WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
        WHEN weight >= 140 THEN 'Truck_Required'
    END;

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

-- ############   task 5  ################# --
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING (product_price::float)

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING (weight::float)

ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(17)

ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(11)

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING (date_added::date)

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING (uuid::uuid)

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOL USING (
    CASE 
        WHEN still_available = 'Still_avaliable' THEN TRUE
        WHEN still_available = 'Removed' THEN FALSE
    END
)

ALTER TABLE dim_date_times
ALTER COLUMN weight_class TYPE VARCHAR(14);


-- ############   task 6  ################# --
SELECT * FROM dim_date_times

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2)

ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(4)

ALTER TABLE dim_date_times
ALTER COLUMN day TYPE VARCHAR(2)

ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10)

ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;


-- ############   task 7  ################# --
SELECT max(length(cast(  expiry_date as VARCHAR))) FROM dim_card_details

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(22)

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(19)

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE


-- ############   task 8  ################# --
--create prime key in each dim table whose column contained in orders table
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

--Set foreign key to orders_table, link them
ALTER TABLE orders_table
ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number)

ALTER TABLE orders_table
ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid)

ALTER TABLE orders_table
ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code)

ALTER TABLE orders_table
ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code)

ALTER TABLE orders_table
ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid)
