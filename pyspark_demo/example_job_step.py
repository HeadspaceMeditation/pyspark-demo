from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql.functions import sum


@dataclass
class InputEvent:
    """
    A hypothetical input event into our Spark job step.
    In a real "pipeline" this might be something like an Amplitude event and we'd eventually have one of these for
    every type of event we work with.

    ^ For complex input / output types it does take a bit of effort to write a data class for each type of input record -- but:

    1. For many types of source data we'd be able to automatically code generate these from the source schema (e.g. JSON, Protobufs etc).
    2. the team can reuse them across projects (e.g. if you're writing a new a gold table, you can use all the silver data classes in your unit tests).
    """

    user_id: str = "user-1"
    product_id: str = "product-1"
    timestamp: int = 1
    price: int = 100


class ExampleJobStep:
    """
    An example step in a larger Spark Job. Note how it consumes and returns a Dataframe
    which makes this job class "pure" (i.e. it has no-side effects), and therefore easy to unit test in isolation.

    This class could easily be imported and executed within one of our larger "notebooks". And you could even break down larger notebooks into
    multiple job steps, allowing you test each piece in isolation.
    """

    def run(
        self, input: DataFrame  # type: InputEvent
    ) -> DataFrame:
        return input.groupBy("product_id").agg(sum("price").alias("sales"))
