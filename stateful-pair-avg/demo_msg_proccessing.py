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
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, FlatMapFunction, RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings, Schema

class CountWindowAverage(FlatMapFunction):

    def __init__(self):
        self.sum = None

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor(
            "average",  # the state name
            Types.PICKLED_BYTE_ARRAY()  # type information
        )
        self.sum = runtime_context.get_state(descriptor)

    def flat_map(self, value):
        # access the state value
        current_sum = self.sum.value() # Access the value of the state stored in self.sum
        if current_sum is None:
            current_sum = (0, 0) # Entries correspond to number of elements summed and the value of the sum respectively

        # update the count
        current_sum = (current_sum[0] + 1, current_sum[1] + value[1])

        # update the state
        self.sum.update(current_sum)

        # if the count reaches 2, emit the average and clear the state
        if current_sum[0] >= 2:
            self.sum.clear()
            yield Row(value[0], int(current_sum[1] / current_sum[0]))

def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_string("pipeline.name", "Extended Pipeline: Average Pairs of Binary Numbers")
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

    create_kafka_source_ddl = """
            CREATE TABLE random_binary_msg (
                createTime VARCHAR,
                binaryInt TINYINT
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'random_binary_msg',
              'properties.bootstrap.servers' = 'kafka:9092',
              'properties.group.id' = 'test_3',
              'scan.startup.mode' = 'latest-offset',
              'value.format' = 'json'
            )
            """

    create_es_sink_ddl = """
            CREATE TABLE es_sink (
                createTime VARCHAR,
                averageInt INT
            ) with (
                'connector' = 'elasticsearch-7',
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'platform_avg_1',
                'sink.flush-on-checkpoint' = 'true',
                'document-id.key-delimiter' = '$',
                'sink.bulk-flush.max-size' = '42mb',
                'sink.bulk-flush.max-actions' = '32',
                'sink.bulk-flush.interval' = '1000',
                'sink.bulk-flush.backoff.delay' = '1000',
                'format' = 'json'
            )
    """
    print("Printing result to stdout. Use --output to specify output path.")

    # Sets up Table API calls
    t_env.execute_sql(create_kafka_source_ddl)
    t_env.execute_sql(create_es_sink_ddl)

    # Create Table using the source
    table = t_env.from_path("random_binary_msg")

    # Convert Table to Datastream
    ds = t_env.to_data_stream(table)

    # Use Datastream API stateful function
    ds = ds.key_by(lambda row: 1) \
            .flat_map(CountWindowAverage(), output_type=Types.ROW([Types.STRING(), Types.INT()])) \

    # Convert Datastream back to table
    table_out = t_env.from_data_stream(ds).alias("binaryInt")
    # Write to sink
    table_out.execute_insert("es_sink")

if __name__ == '__main__':
    log_processing()