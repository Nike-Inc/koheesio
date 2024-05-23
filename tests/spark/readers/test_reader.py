from koheesio.spark.readers.dummy import DummyReader


def test_reader(spark):
    test_reader = DummyReader(range=1)
    actual = test_reader.read().head().asDict()
    expected = {"id": 0}
    assert actual == expected


def test_execute(spark):
    test_reader = DummyReader(range=1)
    actual = test_reader.execute().df.head().asDict()
    expected = {"id": 0}
    assert actual == expected
