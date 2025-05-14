import pytest

from koheesio.spark.readers.jdbc import JdbcReader

pytestmark = pytest.mark.spark


class TestJdbcReader:
    common_options = {
        "driver": "driver",
        "url": "url",
        "user": "user",
        "password": "password",
    }

    def test_get_options_wo_extra_options(self):
        jr = JdbcReader(**self.common_options, dbtable="table")
        actual = jr.get_options()
        del actual["password"]  # we don't need to test for this

        expected = {**self.common_options, "dbtable": "table"}
        del expected["password"]  # we don't need to test for this

        assert actual == expected

    def test_get_options_w_extra_options(self):
        jr = JdbcReader(
            options={
                "foo": "foo",
                "bar": "bar",
            },
            query="unit test",
            dbtable="table",
            **self.common_options,
        )

        actual = jr.get_options()
        del actual["password"]  # we don't need to test for this

        expected = {
            **self.common_options,
            "query": "unit test",
            "foo": "foo",
            "bar": "bar",
        }
        del expected["password"]  # we don't need to test for this

        assert sorted(actual) == sorted(expected)

        with pytest.raises(KeyError) as e:
            actual["dbtable"]
            assert e.type is KeyError

    def test_execute_wo_dbtable_and_query(self):
        with pytest.raises(ValueError) as e:
            _ = JdbcReader(**self.common_options)
            assert e.type is ValueError

    def test_execute_w_dbtable_and_query(self, dummy_spark):
        """query should take precedence over dbtable"""
        jr = JdbcReader(**self.common_options, dbtable="foo", query="bar")
        jr.execute()

        assert jr.df.count() == 3
        assert dummy_spark.options_dict["query"] == "bar"
        assert dummy_spark.options_dict.get("dbtable") is None

    def test_execute_w_dbtable(self, dummy_spark):
        """check that dbtable is passed to the reader correctly"""
        jr = JdbcReader(**self.common_options, dbtable="foo")
        jr.execute()

        assert jr.df.count() == 3
        assert dummy_spark.options_dict["dbtable"] == "foo"
