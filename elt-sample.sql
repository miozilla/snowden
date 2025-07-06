BEGIN
    -- Step 1: Create staging table
    CREATE OR REPLACE TABLE staging_orders (
        order_id INT,
        customer_id INT,
        order_date DATE,
        amount FLOAT
    );

    -- Step 2: Load raw data (simulated insert)
    INSERT INTO staging_orders VALUES
        (101, 1, '2025-07-01', 250.00),
        (102, 2, '2025-07-02', 300.00),
        (103, 1, '2025-07-03', 150.00);

    -- Step 3: Create transformed table
    CREATE OR REPLACE TABLE transformed_orders AS
    SELECT
        order_id,
        customer_id,
        order_date,
        amount,
        CURRENT_DATE AS processed_date
    FROM staging_orders
    WHERE amount > 200;

    -- Step 4: Create final table
    CREATE OR REPLACE TABLE final_orders (
        order_id INT PRIMARY KEY,
        customer_id INT,
        order_date DATE,
        amount FLOAT,
        processed_date DATE
    );

    -- Step 5: Merge into final table
    MERGE INTO final_orders tgt
    USING transformed_orders src
    ON tgt.order_id = src.order_id
    WHEN MATCHED THEN UPDATE SET
        tgt.customer_id = src.customer_id,
        tgt.order_date = src.order_date,
        tgt.amount = src.amount,
        tgt.processed_date = src.processed_date
    WHEN NOT MATCHED THEN INSERT (
        order_id, customer_id, order_date, amount, processed_date
    ) VALUES (
        src.order_id, src.customer_id, src.order_date, src.amount, src.processed_date
    );
END;