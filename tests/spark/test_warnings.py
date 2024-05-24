import warnings

import pytest

from koheesio.spark.writers import BatchOutputMode

pytestmark = pytest.mark.spark


@pytest.mark.parametrize("output_mode", [(BatchOutputMode.APPEND)])
def test_muted_warnings(output_mode, dummy_df, spark):
    table_name = "test_table"

    with warnings.catch_warnings(record=True) as w:
        from koheesio.spark.writers.delta import DeltaTableWriter

        DeltaTableWriter(table=table_name, output_mode=output_mode, df=dummy_df)
        params_warnings = []

        for warning in w:
            # UserWarning: Field name "params" shadows an attribute in parent "DeltaTableWriter";
            if str(warning.message).startswith('Field name "params" shadows an attribute in parent'):
                print(str(warning.message))
                params_warnings.append(warning)

        assert len(params_warnings) == 0
