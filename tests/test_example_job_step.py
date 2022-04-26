from typing import List

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark_demo.example_job_step import ExampleJobStep, InputEvent


class TestExampleJobStep:
    """
    A test suite for our Spark job that runs Spark locally and asserts against the output.

    We create input events in memory, per test. This gives two main benefits over hardcoded fixture files:

    1. The person reading the code can easily see all the  input data for the test
    2. It makes tests easy to refactor because each test is largely "self-contained"

    ^ If you're worried about the verbosity required to construct in memory events for large tables,
      it's easy to extract these sorts of things into helper methods with "nice" defaults so you only have to specify
      the specific fields that matter for your test e.g. a_user_with_an_attributed_install(campaign_id="123", ...)

    """

    spark = SparkSession.builder.getOrCreate()  # a local spark session

    def test_sums_a_single_event(self):
        events = [InputEvent(price=100)]
        assert self.run(events) == [Row(product_id="product-1", sales=100)]

    def test_sums_events_by_product_id(self):
        events = [
            InputEvent(product_id="product-1", price=100),
            InputEvent(product_id="product-1", price=200),
            InputEvent(product_id="product-2", price=50),
        ]

        assert self.run(events) == [
            Row(product_id="product-1", sales=300),
            Row(product_id="product-2", sales=50),
        ]

    def run(self, input: List[InputEvent]) -> List[Row]:
        """
        Converts an in-memory Python list to a DataFrame of Row objects.
        PySpark automatically transforms the field names of our InputEvent dataclass
        into columns names
        """
        df = self.spark.sparkContext.parallelize(input).toDF()
        return ExampleJobStep().run(df).collect()
