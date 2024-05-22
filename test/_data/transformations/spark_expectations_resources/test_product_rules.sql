
insert into default.test_product_rules (product_id, table_name, rule_type, rule, column_name,
expectation, action_if_failed, tag, description, enable_for_source_dq_validation,  enable_for_target_dq_validation,
is_active, enable_error_drop_alert, error_drop_threshold) values

-- check for nulls in column col1
('test_product', 'default.output_table', 'row_dq', 'col1_is_not_null', 'col1',
'col1 is not null', 'drop', 'validity', 'col1 should not be null', false, false, true, false, false),

-- check for correct type of col2
('test_product', 'default.output_table', 'row_dq', 'col2_is_not_null', 'col2',
'col2 is not null', 'drop', 'validity', 'col2 should not be null', false, false, true, false, false)