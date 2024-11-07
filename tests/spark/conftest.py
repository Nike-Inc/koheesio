from koheesio.spark.utils.testing import (
    fixture,
    register_fixture,
    register_fixtures,
    set_env_vars,
    setup_test_data,
    spark,
)

register_fixtures(spark, scope="session")

@fixture(scope="session", autouse=True)
def setup(set_env_vars, spark, delta_file):
    db_name = "klettern"

    if not spark.catalog.databaseExists(db_name):
        spark.sql(f"CREATE DATABASE {db_name}")
        spark.sql(f"USE {db_name}")

    # TODO: change setup_test_data so that we don't have to pass a Path object (str_or_path)
    setup_test_data(spark=spark, delta_file=delta_file)
    yield
