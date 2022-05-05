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
from pyflink.common import WatermarkStrategy, Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, FlatMapFunction, RuntimeContext, \
    MapFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings, Schema
import random


class SpoofRfiFlagger(FlatMapFunction):
    # Class used for identifying the longest consecutive run of integers in a datastream,
    # a single integer does not constitute a run.

    def __init__(self):
        self.sum = None
        self.max_lengths = None

    def open(self, runtime_context: RuntimeContext):
        # Useful to consult :
        # https://nightlies.apache.org/flink/flink-docs-release-1.13/api/python/_modules/pyflink/datastream/state.html

        # Tuple to store a running tally of how many consecutive values have appeared
        descriptor = ValueStateDescriptor(
            "runInt",  # the state name
            Types.PICKLED_BYTE_ARRAY()  # type information
        )
        self.sum = runtime_context.get_state(descriptor)

        # Dictionary to keep track of the longest runs seen
        descriptor_max = MapStateDescriptor(
            "runLength",  # the state name
            Types.INT(),  # Key type
            Types.INT()  # Value type
        )
        self.max_lengths = runtime_context.get_state(descriptor_max)

    def flat_map(self, value):
        # access the state value
        current_run = self.sum.value()  # Access the value of the state stored in self.sum
        if current_run is None:
            current_run = (-1, 0)  # Entries correspond to (0) previous entry and (1) number of previous entries in run.

        max_lengths = self.max_lengths.value()  # Access the value of the state stored in self.sum
        if max_lengths is None:
            max_lengths = {}

        flag = random.randint()
        creation_time = value[0]
        baselineId = value[1]
        signalValue = value[2]

        yield Row(creation_time, baselineId, signalValue, flag)


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
                runInt INT,
                runLength INT
            ) with (
                'connector' = 'elasticsearch-7',
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'platform_run_2',
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

    # Create Table using the source
    table = t_env.from_path("baseline_signal_source")

    # Convert Table to Datastream
    ds = t_env.to_data_stream(table)

    # Use Datastream API stateful function
    # Key the streams by the baselineId
    ds = ds.key_by(lambda row: row[1]) \
        .flat_map(SpoofRfiFlagger(), output_type=Types.ROW([Types.STRING(), Types.INT(), Types.FLOAT(), Types.INT()]))
    # Convert Datastream back to table
    table_out = t_env.from_data_stream(ds).alias("binaryInt")
    # Write to sink
    table_out.execute_insert("es_sink")


if __name__ == '__main__':
    log_processing()
