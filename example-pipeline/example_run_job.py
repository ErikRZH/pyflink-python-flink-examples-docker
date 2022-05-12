################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from pyflink.common import WatermarkStrategy, Row, Time
from pyflink.common.typeinfo import Types
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, FlatMapFunction, RuntimeContext, \
    MapFunction, ProcessWindowFunction, WindowAssigner, Trigger
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings, Schema, TableDescriptor
import random
from pyflink.datastream.window import TimeWindow, TimeWindowSerializer
from pyflink.table.window import Tumble
from pyflink.table.expressions import lit, col

from typing import Iterable, Tuple
import statistics

class SpoofRfiFlagger(FlatMapFunction):
    # Class used for identifying the longest consecutive run of integers in a datastream,
    # a single integer does not constitute a run.
    def flat_map(self, value):

        flag = random.randint(0, 1)
        creation_time = value[0]
        baselineId = value[1]
        signalValue = value[2]

        yield Row(creation_time, baselineId, signalValue, flag)


# Window function to work out QA metrics on sliding windows
# https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/streaming/api/functions/windowing/ProcessWindowFunction.Context.html
# class SummaryWindowProcessFunction(ProcessWindowFunction[tuple, tuple, int, TimeWindow]):
#     # Returns Baseline id: INT , mean: FLOAT, max: FLOAT, min: FLOAT, n_elements: INT, n_flags = INT, windows-start time: INT, window-end time: INT
#     def process(self,
#                 key: int,
#                 context: ProcessWindowFunction.Context[TimeWindow],
#                 elements: Iterable[tuple]) -> Iterable[tuple]:
#         data_values = [e[2] for e in elements] # Get the part of the input which contain the data
#         flagger_values = [e[3] for e in elements] # Get the flags
#
#         return [(key, statistics.mean(data_values), max(data_values), min(data_values), len(data_values), sum(flagger_values), context.window().start, context.window().end)]
#
#     def clear(self, context: ProcessWindowFunction.Context) -> None:
#         pass

def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(restart_attempts=60, delay_between_attempts=int(2*1e3))) #since delay is in milliseconds
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_string("pipeline.name",
                                                      "Extended Pipeline: Longest Run of Binary Numbers")
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

    create_kafka_source_ddl = """
            CREATE TABLE baseline_signal_source (
                createTime VARCHAR,
                baselineId INT,
                signalValue FLOAT
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'baseline_signal',
              'properties.bootstrap.servers' = 'kafka:9092',
              'properties.group.id' = 'test_3',
              'scan.startup.mode' = 'latest-offset',
              'value.format' = 'json'
            )
            """

    create_es_sink_ddl = """
            CREATE TABLE es_sink (
                ts TIMESTAMP(3),
                baselineId INT,
                signalValue FLOAT,
                flagId INT
            ) with (
                'connector' = 'elasticsearch-7',
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'example_pipeline_2',
                'sink.flush-on-checkpoint' = 'true',
                'document-id.key-delimiter' = '$',
                'sink.bulk-flush.max-size' = '42mb',
                'sink.bulk-flush.max-actions' = '32',
                'sink.bulk-flush.interval' = '1000',
                'sink.bulk-flush.backoff.delay' = '1000',
                'format' = 'json'
            )
    """

    create_es_summary_sink_ddl = """
            CREATE TABLE es_summary_sink (
                baselineId INT,
                winMean FLOAT, 
                winMax FLOAT, 
                winMin FLOAT, 
                nElements BIGINT,
                nFlags INT, 
                windowsStartTime TIMESTAMP(3),
                windowEndTime TIMESTAMP(3)
            ) with (
                'connector' = 'elasticsearch-7',
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'example_pipeline_summary_1',
                'sink.flush-on-checkpoint' = 'true',
                'document-id.key-delimiter' = '$',
                'sink.bulk-flush.max-size' = '512mb',
                'sink.bulk-flush.max-actions' = '1024',
                'sink.bulk-flush.interval' = '10000',
                'sink.bulk-flush.backoff.delay' = '10000',
                'format' = 'json'
            )
    """

    # Sets up Table API calls
    t_env.execute_sql(create_kafka_source_ddl)
    t_env.execute_sql(create_es_sink_ddl)
    t_env.execute_sql(create_es_summary_sink_ddl)

    # Create Table using the source
    table = t_env.from_path("baseline_signal_source")

    # Convert Table to Datastream
    ds = t_env.to_data_stream(table)

    # Use Datastream API stateful function
    # Key the streams by the baselineId
    ds = ds.key_by(lambda row: row[1])
    # Spoof RFI flagging
    ds_flagged = ds.flat_map(SpoofRfiFlagger(), output_type=Types.ROW([Types.STRING(), Types.INT(), Types.FLOAT(), Types.INT()]))


    # Convert back to Table for Summary Statistics
    # Tumbling QA window, that is that entries appear once.
    # Convert Datastream back to table
    table_flagged = t_env.from_data_stream(ds_flagged,
        Schema.new_builder()
              .column_by_expression("ts", "CAST(f0 AS TIMESTAMP(3))")
              .column("f1", DataTypes.INT())
              .column("f2", DataTypes.FLOAT())
              .column("f3", DataTypes.INT())
              .watermark("ts", "ts - INTERVAL '3' SECOND")
              .build()
    ).alias("ts, baselineId, signalValue, flagId")
    # Write to sink

    # Groups the rows based on timestamps within 15 seconds of one another, stores the window reference in a "column" w
    # Using expressions from https://nightlies.apache.org/flink/flink-docs-stable/api/python/_modules/pyflink/table/expression.html
    table_out = table_flagged.window(Tumble.over(lit(15).seconds).on(col("ts")).alias("w")) \
                  .group_by(table_flagged.baselineId, col('w')) \
                  .select(table_flagged.baselineId, table_flagged.signalValue.avg, table_flagged.signalValue.max,
                          table_flagged.signalValue.min, table_flagged.baselineId.count, table_flagged.flagId.sum,
                          col("w").start, col("w").end)

    table_out.execute_insert("es_summary_sink")


if __name__ == '__main__':
    log_processing()
