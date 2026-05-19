CREATE TABLE IF NOT EXISTS sales_summary (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    category VARCHAR(50),
    total_sales DECIMAL(10, 2),
    PRIMARY KEY (window_start, category)
);
