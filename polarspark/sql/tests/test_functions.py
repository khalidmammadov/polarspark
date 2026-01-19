#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import math
import unittest

from polarspark.sql import Row
from polarspark.sql.functions import (
    col,
    lit,
    sin,
    cos,
    tan,
    asin,
    acos,
    atan,
    sinh,
    cosh,
    tanh,
    asinh,
    acosh,
    atanh,
    csc,
    sec,
    cot,
    degrees,
    radians,
    cbrt,
)
from polarspark.testing.sqlutils import ReusedSQLTestCase


class MathFunctionsSuite(ReusedSQLTestCase):
    """Test suite for mathematical functions."""

    @staticmethod
    def assert_equal(this, other):
        """Custom assert to compare only values in Rows. i.e. not the field names."""
        this_values = [row[0] for row in this]
        other_values = [row[0] for row in other]
        assert len(this_values) == len(other_values), f"Length mismatch: {len(this_values)} != {len(other_values)}"
        for i, (tv, ov) in enumerate(zip(this_values, other_values)):
            if tv is None and ov is None:
                continue
            elif tv is None or ov is None:
                assert False, f"Index {i}: {tv} != {ov}"
            elif isinstance(tv, float) and isinstance(ov, float) and math.isnan(tv) and math.isnan(ov):
                continue
            elif isinstance(tv, float) and isinstance(ov, float):
                assert math.isclose(tv, ov, rel_tol=1e-9, abs_tol=1e-9), f"Index {i}: {tv} != {ov}"
            else:
                assert tv == ov, f"Index {i}: {tv} != {ov}"

    @classmethod
    def setUpClass(cls):
        super(MathFunctionsSuite, cls).setUpClass()
        # Create test data
        cls.double_data = cls.spark.createDataFrame(
            [(i * 0.2 - 1, i * -0.2 + 1) for i in range(1, 11)], ["a", "b"]
        )
        cls.nn_double_data = cls.spark.createDataFrame(
            [(i * 0.1, i * -0.1) for i in range(1, 11)], ["a", "b"]
        )
        cls.null_doubles = cls.spark.createDataFrame(
            [(1.0,), (2.0,), (3.0,), (None,)], ["a"]
        )

    def _test_one_to_one_math_function(self, spark_func, python_func):
        """
        Test a unary math function against a Python reference implementation.

        Args:
            spark_func: Spark SQL function (e.g., sin)
            python_func: Python function for expected values (e.g., math.sin)
        """
        # Test with column 'a'
        result = self.double_data.select(spark_func(col("a"))).collect()
        expected = [Row(python_func(i * 0.2 - 1)) for i in range(1, 11)]
        self.assert_equal(result, expected)

        # Test with column 'b'
        result = self.double_data.select(spark_func(col("b"))).collect()
        expected = [Row(python_func(-i * 0.2 + 1)) for i in range(1, 11)]
        self.assert_equal(result, expected)

        # Test with null
        # !!! Polars: trigonometry operation not supported for dtype `null`
        # result = self.double_data.select(spark_func(lit(None))).collect()
        # expected = [Row(None) for _ in range(1, 11)]
        # self.assertEqual(result, expected)

    def _test_one_to_one_non_negative_math_function(self, spark_func, python_func):
        """
        Test a unary math function that requires non-negative inputs.

        Args:
            spark_func: Spark SQL function
            python_func: Python function for expected values
        """
        # Test with column 'a' (positive values)
        result = self.nn_double_data.select(spark_func(col("a"))).collect()
        expected = [Row(python_func(i * 0.1)) for i in range(1, 11)]
        self.assert_equal(result, expected)

        # Test with column 'b' (negative values) - special handling for log1p
        if python_func(-1) == math.log1p(-1):
            result = self.nn_double_data.select(spark_func(col("b"))).collect()
            expected = [Row(python_func(i * -0.1)) for i in range(1, 9)] + [Row(None)]
            self.assert_equal(result, expected)

        # Test with null
        result = self.nn_double_data.select(spark_func(lit(None))).collect()
        expected = [Row(None) for _ in range(1, 11)]
        self.assert_equal(result, expected)

    def test_sin(self):
        """Test sin function."""
        self._test_one_to_one_math_function(sin, math.sin)

    def test_csc(self):
        """Test csc (cosecant) function."""
        def py_csc(x):
            try:
                return 1/math.sin(x)
            except ZeroDivisionError:
                return float('inf')
        self._test_one_to_one_math_function(csc, py_csc)

    def test_asin(self):
        """Test asin (arcsine) function."""
        self._test_one_to_one_math_function(asin, math.asin)

    def test_sinh(self):
        """Test sinh (hyperbolic sine) function."""
        self._test_one_to_one_math_function(sinh, math.sinh)

    def test_asinh(self):
        """Test asinh (inverse hyperbolic sine) function."""
        self._test_one_to_one_math_function(
            asinh, lambda x: math.log(x + math.sqrt(x * x + 1))
        )

    def test_cos(self):
        """Test cos function."""
        self._test_one_to_one_math_function(cos, math.cos)

    def test_sec(self):
        """Test sec (secant) function."""
        self._test_one_to_one_math_function(sec, lambda x: 1 / math.cos(x))

    def test_acos(self):
        """Test acos (arccosine) function."""
        self._test_one_to_one_math_function(acos, math.acos)

    def test_cosh(self):
        """Test cosh (hyperbolic cosine) function."""
        self._test_one_to_one_math_function(cosh, math.cosh)

    def test_acosh(self):
        """Test acosh (inverse hyperbolic cosine) function."""
        def check_acosh_domain(x):
            if x < 1:
                return math.nan
            return math.log(x + math.sqrt(x * x - 1))
        self._test_one_to_one_math_function(
            acosh, check_acosh_domain
        )

    def test_tan(self):
        """Test tan function."""
        self._test_one_to_one_math_function(tan, math.tan)

    def test_cot(self):
        """Test cot (cotangent) function."""
        def py_cot(x):
            try:
                return 1/math.tan(x)
            except ZeroDivisionError:
                return float('inf')
        self._test_one_to_one_math_function(cot, py_cot)

    def test_atan(self):
        """Test atan (arctangent) function."""
        self._test_one_to_one_math_function(atan, math.atan)

    def test_tanh(self):
        """Test tanh (hyperbolic tangent) function."""
        self._test_one_to_one_math_function(tanh, math.tanh)

    def test_atanh(self):
        """Test atanh (inverse hyperbolic tangent) function."""
        def atanh_domain_check(x):
            if x < -1 or x > 1:
                return math.nan
            elif x == 1:
                return float('inf')
            elif x == -1:
                return float('-inf')
            return 0.5 * (math.log1p(x) - math.log1p(-x))
        self._test_one_to_one_math_function(
            atanh, atanh_domain_check
        )

    def test_degrees(self):
        """Test degrees function."""
        self._test_one_to_one_math_function(degrees, math.degrees)

        # Additional test using SQL
        result = self.spark.sql(
            "SELECT degrees(0), degrees(1), degrees(1.5)"
        ).collect()
        expected = self.spark.range(1, 3).select(
            degrees(lit(0)), degrees(lit(1)), degrees(lit(1.5))
        ).collect()
        self.assertEqual(result, expected)

    def test_radians(self):
        """Test radians function."""
        self._test_one_to_one_math_function(radians, math.radians)

        # Additional test using SQL
        result = self.spark.sql(
            "SELECT radians(0), radians(1), radians(1.5)"
        ).collect()
        expected = self.spark.range(1, 3).select(
            radians(lit(0)), radians(lit(1)), radians(lit(1.5))
        ).collect()
        self.assertEqual(result, expected)

    def test_cbrt(self):
        """Test cbrt (cube root) function."""

        def cbrt_func(x):
            """Calculate cube root handling negative numbers."""
            if x >= 0:
                return x ** (1 / 3)
            else:
                return -((-x) ** (1 / 3))

        self._test_one_to_one_math_function(cbrt, cbrt_func)


if __name__ == "__main__":
    from polarspark.sql.tests.test_functions import *  # noqa: F401, F403

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
