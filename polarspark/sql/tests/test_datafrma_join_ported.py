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
import unittest

from polarspark.sql import Row
from polarspark.sql.functions import col
from polarspark.testing.sqlutils import ReusedSQLTestCase


class DataFrameJoinTestsMixin:
    def _assert_rows_equal(self, result, expected, row_desc=""):
        """Helper method to assert that result rows match expected values."""
        self.assertEqual(len(result), len(expected))
        for i, (res, exp) in enumerate(zip(result, expected)):
            for j, (res_val, exp_val) in enumerate(zip(res, exp)):
                self.assertEqual(res_val, exp_val, f"Row {i}, column {j}{row_desc}: value mismatch")

    def test_join_using(self):
        """Test join - join using"""
        df = self.spark.createDataFrame([(i, str(i)) for i in [1, 2, 3]], ["int", "str"])
        df2 = self.spark.createDataFrame([(i, str(i + 1)) for i in [1, 2, 3]], ["int", "str"])

        result = df.join(df2, "int").collect()
        expected = [(1, "1", "2"), (2, "2", "3"), (3, "3", "4")]

        self._assert_rows_equal(result, expected)

    def test_join_using_multiple_columns(self):
        """Test join - join using multiple columns"""
        df = self.spark.createDataFrame(
            [(i, i + 1, str(i)) for i in [1, 2, 3]], ["int", "int2", "str"]
        )
        df2 = self.spark.createDataFrame(
            [(i, i + 1, str(i + 1)) for i in [1, 2, 3]], ["int", "int2", "str"]
        )

        result = df.join(df2, ["int", "int2"]).collect()
        expected = [(1, 2, "1", "2"), (2, 3, "2", "3"), (3, 4, "3", "4")]

        self._assert_rows_equal(result, expected)

    def test_join_using_multiple_columns_array(self):
        """Test join - join using multiple columns array"""
        df = self.spark.createDataFrame(
            [(i, i + 1, str(i)) for i in [1, 2, 3]], ["int", "int2", "str"]
        )
        df2 = self.spark.createDataFrame(
            [(i, i + 1, str(i + 1)) for i in [1, 2, 3]], ["int", "int2", "str"]
        )

        result = df.join(df2, ["int", "int2"]).collect()
        expected = [(1, 2, "1", "2"), (2, 3, "2", "3"), (3, 4, "3", "4")]

        self._assert_rows_equal(result, expected)

    # def test_join_sorted_columns_not_in_output_set(self):
    #     """Test join - sorted columns not in join's outputSet"""
    #     df = self.spark.createDataFrame([(1, 2, "1"), (3, 4, "3")], ["int", "int2", "str_sort"]).alias("df1")
    #     df2 = self.spark.createDataFrame([(1, 3, "1"), (5, 6, "5")], ["int", "int2", "str"]).alias("df2")
    #     df3 = self.spark.createDataFrame([(1, 3, "1"), (5, 6, "5")], ["int", "int2", "str"]).alias("df3")

    #     result = df.join(df2, col("df1.int") == col("df2.int"), "outer") \
    #         .select(col("df1.int"), col("df2.int2")) \
    #         .orderBy(col("str_sort").asc(), col("str").asc()) \
    #         .collect()
    #     expected = [(None, 6), (1, 3), (3, None)]
    #     self._assert_rows_equal(result, expected)

    #     result2 = df2.join(df3, col("df2.int") == col("df3.int"), "inner") \
    #         .select(col("df2.int"), col("df3.int")) \
    #         .orderBy(col("df2.str").desc()) \
    #         .collect()
    #     expected2 = [(5, 5), (1, 1)]
    #     self._assert_rows_equal(result2, expected2)

    def test_join_using_specifying_join_type(self):
        """Test join - join using specifying join type"""
        df = self.spark.createDataFrame([(i, str(i)) for i in [1, 2, 3]], ["int", "str"])
        df2 = self.spark.createDataFrame([(i, str(i + 1)) for i in [1, 2, 3]], ["int", "str"])

        result = df.join(df2, "int", "inner").collect()
        expected = [(1, "1", "2"), (2, "2", "3"), (3, "3", "4")]
        self._assert_rows_equal(result, expected)

    def test_join_using_multiple_columns_and_specifying_join_type(self):
        """Test join - join using multiple columns and specifying join type"""
        df = self.spark.createDataFrame([(1, 2, "1"), (3, 4, "3")], ["int", "int2", "str"])
        df2 = self.spark.createDataFrame([(1, 3, "1"), (5, 6, "5")], ["int", "int2", "str"])

        # inner join
        result = df.join(df2, ["int", "str"], "inner").collect()
        expected = [(1, "1", 2, 3)]
        self._assert_rows_equal(result, expected)

        # left join
        result = df.join(df2, ["int", "str"], "left").collect()
        expected = [(1, "1", 2, 3), (3, "3", 4, None)]
        self._assert_rows_equal(result, expected)

        # right join
        result = df.join(df2, ["int", "str"], "right").collect()
        expected = [(1, "1", 2, 3), (5, "5", None, 6)]
        self._assert_rows_equal(result, expected)

        # outer join
        result = df.join(df2, ["int", "str"], "outer").collect()
        df.join(df2, ["int", "str"], "outer").show()
        expected = [(1, "1", 2, 3), (3, "3", 4, None), (5, "5", None, 6)]
        self._assert_rows_equal(result, expected)

        # left_semi join
        result = df.join(df2, ["int", "str"], "left_semi").collect()
        expected = [(1, "1", 2)]
        self._assert_rows_equal(result, expected)

        # semi join (alias for left_semi)
        result = df.join(df2, ["int", "str"], "semi").collect()
        expected = [(1, "1", 2)]
        self._assert_rows_equal(result, expected)

        # left_anti join
        result = df.join(df2, ["int", "str"], "left_anti").collect()
        expected = [(3, "3", 4)]
        self._assert_rows_equal(result, expected)

        # anti join (alias for left_anti)
        result = df.join(df2, ["int", "str"], "anti").collect()
        expected = [(3, "3", 4)]
        self._assert_rows_equal(result, expected)


class DataFrameJoinTests(DataFrameJoinTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from polarspark.sql.tests.test_datafrma_join_ported import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
