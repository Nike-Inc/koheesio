create table if not exists default.test_product_rules (
    product_id STRING,
    table_name STRING,
    rule_type STRING,
    rule STRING,
    column_name STRING,
    expectation STRING,
    action_if_failed STRING,
    tag STRING,
    description STRING,
    enable_for_source_dq_validation BOOLEAN,
    enable_for_target_dq_validation BOOLEAN,
    is_active BOOLEAN,
    enable_error_drop_alert BOOLEAN,
    error_drop_threshold INT
) using delta