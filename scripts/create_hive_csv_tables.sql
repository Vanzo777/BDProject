-- Создание external таблиц в Hive для чтения CSV из MinIO

CREATE SCHEMA IF NOT EXISTS hive.default;

-- Таблица для order_classification.csv
DROP TABLE IF EXISTS hive.default.order_classification_csv;

CREATE EXTERNAL TABLE hive.default.order_classification_csv (
    order_id VARCHAR,
    order_book_id VARCHAR,
    side VARCHAR,
    timestamp VARCHAR,
    created_on VARCHAR,
    initial_qty VARCHAR,
    created_with_qty VARCHAR,
    qty_on_end VARCHAR,
    filled_on_start VARCHAR,
    deleted VARCHAR,
    fully_executed VARCHAR,
    partially_executed VARCHAR,
    existed_for VARCHAR,
    min_reaction_time VARCHAR,
    modify_count VARCHAR,
    distance_from_bbo_avg VARCHAR,
    distance_from_bbo_max VARCHAR,
    distance_from_bbo_min VARCHAR,
    price_dif_from_bbo_avg VARCHAR,
    price_dif_from_bbo_max VARCHAR,
    price_dif_from_bbo_min VARCHAR,
    priority_count_avg VARCHAR,
    priority_count_max VARCHAR,
    priority_count_min VARCHAR,
    price_level_change_avg VARCHAR,
    price_level_change_max VARCHAR,
    price_level_change_min VARCHAR,
    tick_count_price_level_change_avg VARCHAR,
    tick_count_price_level_change_max VARCHAR,
    tick_count_price_level_change_min VARCHAR,
    time_passed_since_last_event_avg VARCHAR,
    time_passed_since_last_event_max VARCHAR,
    time_passed_since_last_event_min VARCHAR
)
WITH (
    format = 'CSV',
    skip_header_line_count = 1,
    external_location = 's3a://lakehouse/raw/'
);

-- Таблица для product_info.csv
DROP TABLE IF EXISTS hive.default.product_info_csv;

CREATE EXTERNAL TABLE hive.default.product_info_csv (
    product_info_order_book_id VARCHAR,
    product_info_underlying_order_book_id VARCHAR,
    product_family VARCHAR,
    product_info_symbol VARCHAR,
    product_info_long_name VARCHAR,
    product_info_financial_product VARCHAR,
    product_info_put_or_call VARCHAR,
    product_info_strike_price VARCHAR,
    product_info_number_of_decimal_in_price VARCHAR,
    product_info_number_of_decimals_in_strike_price VARCHAR,
    product_info_expiration_date VARCHAR,
    product_info_timestamp VARCHAR,
    product_info_number_of_legs VARCHAR,
    taker_count VARCHAR,
    unique_id_count VARCHAR,
    modify_count VARCHAR,
    ctag_volume VARCHAR,
    etag_volume VARCHAR,
    ptag_volume VARCHAR,
    leg_volume VARCHAR,
    occured_at_cross VARCHAR,
    ticks_0_price_from VARCHAR,
    ticks_0_price_to VARCHAR,
    ticks_0_tick_size VARCHAR,
    ticks_0_timestamp VARCHAR,
    ticks_1_price_from VARCHAR,
    ticks_1_price_to VARCHAR,
    ticks_1_tick_size VARCHAR,
    ticks_1_timestamp VARCHAR,
    ticks_2_price_from VARCHAR,
    ticks_2_price_to VARCHAR,
    ticks_2_tick_size VARCHAR,
    ticks_2_timestamp VARCHAR
)
WITH (
    format = 'CSV',
    skip_header_line_count = 1,
    external_location = 's3a://lakehouse/raw/'
);
