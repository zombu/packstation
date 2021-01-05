# -*- coding: utf-8 -*-

import pytest

from packstation.skeleton import fib

__author__ = "raspberry Pi"
__copyright__ = "raspberry Pi"
__license__ = "mit"


def test_fib():
    assert fib(1) == 1
    assert fib(2) == 1
    assert fib(7) == 13
    with pytest.raises(AssertionError):
        fib(-10)
