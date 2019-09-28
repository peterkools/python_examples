"""
    An interview question requested to code to evaluate this statement:
        testFunc([0, 1, 2, 3, 4, 5]).elemAt(3).shouldBe(equalTo(3))

    This implementation provides additional comparison operators.
"""

import inspect
import sys


class testFunc(object):

    def __init__(self, a):
        self.data = a

    def elemAt(self, position):
        self.position = position
        return self

    def shouldBe(self, comparator):
        operator = comparator[0]
        value = comparator[1]
        if operator == 'equalTo':
            return True if self.data[self.position] == value else False
        elif operator == 'greaterThan':
            return True if self.data[self.position] > value else False
        elif operator == 'lessThan':
            return True if self.data[self.position] < value else False
        elif operator == 'gte':
            return True if self.data[self.position] >= value else False
        elif operator == 'lte':
            return True if self.data[self.position] <= value else False
        return False


def equalTo(value):
    comparator = inspect.currentframe().f_code.co_name
    return (comparator, value)


def greaterThan(value):
    comparator = inspect.currentframe().f_code.co_name
    return (comparator, value)


def lessThan(value):
    comparator = inspect.currentframe().f_code.co_name
    return (comparator, value)


def gte(value):
    comparator = inspect.currentframe().f_code.co_name
    return (comparator, value)


def lte(value):
    comparator = inspect.currentframe().f_code.co_name
    return (comparator, value)


if __name__ == '__main__':
    data = [0, 1, 2, 3, 4, 5]
    assert testFunc(data).elemAt(3).shouldBe(equalTo(3)) is True
    assert testFunc(data).elemAt(3).shouldBe(equalTo(4)) is False
    assert testFunc(data).elemAt(5).shouldBe(greaterThan(3)) is True
    assert testFunc(data).elemAt(5).shouldBe(greaterThan(9)) is False
    assert testFunc(data).elemAt(0).shouldBe(lessThan(1)) is True
    assert testFunc(data).elemAt(0).shouldBe(lessThan(-1)) is False
    assert testFunc(data).elemAt(1).shouldBe(gte(-2)) is True
    assert testFunc(data).elemAt(1).shouldBe(gte(9)) is False
    assert testFunc(data).elemAt(4).shouldBe(lte(9)) is True
    assert testFunc(data).elemAt(4).shouldBe(lte(2)) is False

    print('Success')
