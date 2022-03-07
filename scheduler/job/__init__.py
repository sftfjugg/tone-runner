__all__ = ['get_test_class']

from constant import TestType
from .base_test import BaseTest
from .business_test import BusinessTest
from .function_test import FunctionTest
from .performance_test import PerformanceTest
from .stability_test import StabilityTest


def get_test_class(test_type):
    factory_map = {
        TestType.FUNCTIONAL: FunctionTest,
        TestType.PERFORMANCE: PerformanceTest,
        TestType.BUSINESS: BusinessTest,
        TestType.STABILITY: StabilityTest
    }
    if test_type in factory_map:
        return factory_map[test_type]
    return BaseTest
