from koheesio.utils import get_args_for_func, get_random_string


def test_get_args_for_func():
    def func(a, b, c):
        return a + b + c

    func, args = get_args_for_func(func, {"a": 1, "b": 2, "c": 3})
    assert args == {"a": 1, "b": 2, "c": 3}


def test_import_class():
    import datetime

    from koheesio.utils import import_class

    assert import_class("datetime.datetime") == datetime.datetime


def test_get_random_string():
    assert get_random_string(10) != get_random_string(10)
    assert len(get_random_string(10)) == 10
    assert len(get_random_string(10, "abc")) == 10
    assert get_random_string(10, "abc").startswith("abc_")
