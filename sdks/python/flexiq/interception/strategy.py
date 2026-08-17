"""Interception strategies for argument classification."""

from enum import Enum


class Strategy(Enum):
    """How the interceptor handles an argument."""

    PASS = "pass"
    CONVERT = "convert"
    PROXY = "proxy"
    REDIRECT = "redirect"
    REJECT = "reject"
