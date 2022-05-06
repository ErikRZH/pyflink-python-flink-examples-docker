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
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, FlatMapFunction, RuntimeContext, \
    MapFunction, ProcessWindowFunction, WindowAssigner, Trigger
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings, Schema
import random
from pyflink.datastream.window import TimeWindow, TimeWindowSerializer # In 1.16 use TumblingEventTimeWindows instead!
from pyflink.common.serializer import TypeSerializer
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

# Method for creating a tumbling window
class TumblingEventWindowAssigner(WindowAssigner[Tuple, TimeWindow]):

    def __init__(self, size: int, offset: int, is_event_time: bool):
        self._size = size
        self._offset = offset
        self._is_event_time = is_event_time

    def assign_windows(self,
                       element: Tuple,
                       timestamp: int,
                       context: WindowAssigner.WindowAssignerContext) -> Collection[TimeWindow]:
        start = TimeWindow.get_window_start_with_offset(timestamp, self._offset, self._size)
        return [TimeWindow(start, start + self._size)]

    def get_default_trigger(self, env) -> Trigger[Tuple, TimeWindow]:
        return EventTimeTrigger()

    def get_window_serializer(self) -> TypeSerializer[TimeWindow]:
        return TimeWindowSerializer()

    def is_event_time(self) -> bool:
        return False
# Window function to work out QA metrics on sliding windows
# https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/streaming/api/functions/windowing/ProcessWindowFunction.Context.html
class SummaryWindowProcessFunction(ProcessWindowFunction[tuple, tuple, int, TimeWindow]):
    # Returns Baseline id: INT , mean: FLOAT, max: FLOAT, min: FLOAT, n_elements: INT, n_flags = INT, windows-start time: INT, window-end time: INT
    def process(self,
                key: int,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple]) -> Iterable[tuple]:
        data_values = [e[2] for e in elements] # Get the part of the input which contain the data
        flagger_values = [e[3] for e in elements] # Get the flags

        return [(key, statistics.mean(data_values), max(data_values), min(data_values), len(data_values), sum(flagger_values), context.window().start, context.window().end)]

    def clear(self, context: ProcessWindowFunction.Context) -> None:
        pass

def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
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
                createTime VARCHAR,
                baselineId INT,
                signalValue FLOAT,
                flagId INT
            ) with (
                'connector' = 'elasticsearch-7',
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'example_pipeline_1',
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
                nElements INT,
                nFlags INT, 
                windowsStartTime INT,
                windowEndTime INT
            ) with (
                'connector' = 'elasticsearch-7',
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'example_pipeline_summary_1',
                'sink.flush-on-checkpoint' = 'true',
                'document-id.key-delimiter' = '$',
                'sink.bulk-flush.max-size' = '42mb',
                'sink.bulk-flush.max-actions' = '32',
                'sink.bulk-flush.interval' = '1000',
                'sink.bulk-flush.backoff.delay' = '1000',
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
    ds.flat_map(SpoofRfiFlagger(), output_type=Types.ROW([Types.STRING(), Types.INT(), Types.FLOAT(), Types.INT()]))
    # Tumbling QA window, that is that entries appear once.
    ds.window(TumblingEventTimeWindows.of(Time.seconds(10))) # Time.milliseconds may be better is the sending rate is increased
    # Apply functions to these windows
    # Returns Baseline id: INT , mean: FLOAT, max: FLOAT, min: FLOAT, n_elements: INT, n_flags = INT, windows-start time: INT, window-end time: INT
    ds.process(SummaryWindowProcessFunction(),
                 Types.TUPLE([Types.INT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.INT(), Types.INT(), Types.INT(), Types.INT()]))
    # Convert Datastream back to table
    table_out = t_env.from_data_stream(ds)
    # Write to sink
    table_out.execute_insert("es_summary_sink")


if __name__ == '__main__':
    log_processing()
