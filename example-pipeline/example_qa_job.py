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
import random

from pyflink.common import Row, Configuration
from pyflink.common.typeinfo import Types
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream import StreamExecutionEnvironment, FlatMapFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment, DataTypes, Schema
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.table.window import Tumble
from pyflink.table.expressions import lit, col


class SpoofRfiFlagger(FlatMapFunction):
    # Class used for identifying the longest consecutive run of integers in a datastream,
    # a single integer does not constitute a run.
    def flat_map(self, value):
        flag = random.randint(0, 1)
        creation_time = value[0]
        baselineId = value[1]
        signalValue = value[2]
        yield Row(creation_time, baselineId, signalValue, flag)


class RfiQaRuns(FlatMapFunction):
    # Class used for identifying the longest consecutive run of integers in a datastream,
    # a single integer does not constitute a run.

    def __init__(self):
        self.run = None

    def open(self, runtime_context: RuntimeContext):
        # Useful to consult :
        # https://nightlies.apache.org/flink/flink-docs-release-1.13/api/python/_modules/pyflink/datastream/state.html
        # Tuple to store a running tally of the flag and number of flags seen
        descriptor = ValueStateDescriptor(
            "flagInt",  # the state name
            Types.PICKLED_BYTE_ARRAY()  # type information
        )
        self.run = runtime_context.get_state(descriptor)

    def flat_map(self, value):
        # access the state value
        current_run = self.run.value()  # Access the value of the state stored in self.sum
        if current_run is None:
            current_run = (-1, 0)  # Fields: 0: previous entry, 1: number of previous entries in run.
        seen_flag = value[3]
        seen_time = value[0]

        # if the current flag is positive and the previous was not start recording a rfi
        if seen_flag != current_run[0] and seen_flag == 1:
            current_run = (seen_flag, 1)
            self.run.update(current_run)
        elif seen_flag == current_run[0] and seen_flag == 1:  # current and previous flag are positive, add 1 to length
            current_run = (seen_flag, current_run[1] + 1)
            self.run.update(current_run)
        elif seen_flag != current_run[0] and seen_flag == 0:  # when RFI ends, record its length and when it ended
            self.run.update((seen_flag, 0))  # reset run information
            yield Row(value[1], current_run[1], seen_time)  # baselineId, length of RFI, RFI end time


class RfiQaCurrent(FlatMapFunction):
    # Class used for identifying the longest consecutive run of integers in a datastream,
    # a single integer does not constitute a run.
    def flat_map(self, value):
        flag = value[3]
        baselineId = value[1]

        yield Row(baselineId, flag)


def qa_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    # start a checkpoint every 1000 ms
    env.enable_checkpointing(1000)

    # set mode to exactly-once (this is the default)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

    # make sure 500 ms of progress happen between checkpoints
    env.get_checkpoint_config().set_min_pause_between_checkpoints(500)

    # checkpoints have to complete within one minute, or are discarded
    env.get_checkpoint_config().set_checkpoint_timeout(60000)

    # only two consecutive checkpoint failures are tolerated
    env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(2)

    # allow only one checkpoint to be in progress at the same time
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    env.set_parallelism(5)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(restart_attempts=60,
                                                                   delay_between_attempts=int(2 * 1e3)))  # delay in ms
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_string("pipeline.name",
                                                      "Prototype QA Metric Generation: Spoof Receive")
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)
    # This is so that no downstream process waits for watermarks which kafka does not provide.
    # Without this the pipeline fails when parallelism is greater than 4.
    # I am not sure how kafka topic partitions work, but they are apparently used to generate watermarks at the source.
    t_env.get_config().get_configuration().set_string("table.exec.source.idle-timeout", "1 s")

    create_kafka_source_ddl = """
            CREATE TABLE baseline_signal_source (
                createTime VARCHAR,
                baselineId INT,
                signalValue FLOAT
            ) WITH (
              'connector' = 'kafka',
              'sink.partitioner' = 'round-robin',
              'topic' = 'baseline_signal',
              'properties.bootstrap.servers' = 'kafka:9092',
              'properties.group.id' = 'test_3',
              'scan.startup.mode' = 'latest-offset',
              'value.format' = 'json'
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
                'sink.bulk-flush.max-size' = '2gb',
                'sink.bulk-flush.max-actions' = '320',
                'sink.bulk-flush.interval' = '1s',
                'sink.bulk-flush.backoff.delay' = '10s',
                'format' = 'json'
            )
    """
    create_es_rfi_run_sink_ddl = """
            CREATE TABLE es_rfi_run_sink (
                baselineId INT,
                nRfiSamples INT, 
                rfiEndTime TIMESTAMP(3)
            ) with (
                'connector' = 'elasticsearch-7',
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'example_pipeline_rfi_run_1',
                'sink.flush-on-checkpoint' = 'true',
                'document-id.key-delimiter' = '$',
                'sink.bulk-flush.max-size' = '2gb',
                'sink.bulk-flush.max-actions' = '320',
                'sink.bulk-flush.interval' = '1s',
                'sink.bulk-flush.backoff.delay' = '10s',
                'format' = 'json'
            )
    """
    # Use upsert for the current RFI values, this is the case if a primary key is defined. In this case baselineID
    create_es_rfi_current_sink_ddl = """
            CREATE TABLE es_rfi_current_sink (
                baselineId INT,
                nRfiFlag INT,
            PRIMARY KEY (baselineId) NOT ENFORCED
            ) with (
                'connector' = 'elasticsearch-7',
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'example_pipeline_rfi_current_1',
                'sink.flush-on-checkpoint' = 'true',
                'document-id.key-delimiter' = '$',
                'sink.bulk-flush.max-size' = '2gb',
                'sink.bulk-flush.max-actions' = '320',
                'sink.bulk-flush.interval' = '1s',
                'sink.bulk-flush.backoff.delay' = '10s',
                'format' = 'json'
            )
    """

    create_print_sink_dll = """    
            CREATE TABLE print (
                baselineId INT,
                winMean FLOAT, 
                winMax FLOAT, 
                winMin FLOAT, 
                nElements BIGINT,
                nFlags INT, 
                windowsStartTime TIMESTAMP(3),
                windowEndTime TIMESTAMP(3)
            ) WITH (
                'connector' = 'print',
                'sink.parallelism' = '1'
            )
    """
    # Sets up Table API calls
    t_env.execute_sql(create_kafka_source_ddl)
    t_env.execute_sql(create_es_summary_sink_ddl)
    t_env.execute_sql(create_es_rfi_run_sink_ddl)
    t_env.execute_sql(create_es_rfi_current_sink_ddl)
    t_env.execute_sql(create_print_sink_dll)

    # Create Table using the source
    table = t_env.from_path("baseline_signal_source")

    # Convert Table to Datastream
    ds = t_env.to_data_stream(table)

    # Use Datastream API stateful function
    # Key the streams by the baselineId
    ds = ds.key_by(lambda row: row[1])
    # Spoof RFI flagging
    ds_flagged = ds.flat_map(SpoofRfiFlagger(), output_type=Types.ROW([Types.STRING(), Types.INT(), Types.FLOAT(),
                                                                       Types.INT()]))

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
    # From https://nightlies.apache.org/flink/flink-docs-stable/api/python/_modules/pyflink/table/expression.html
    table_flagged = table_flagged.window(Tumble.over(lit(15).seconds).on(col("ts")).alias("w")) \
        .group_by(table_flagged.baselineId, col('w')) \
        .select(table_flagged.baselineId, table_flagged.signalValue.avg, table_flagged.signalValue.max,
                table_flagged.signalValue.min, table_flagged.baselineId.count, table_flagged.flagId.sum,
                col("w").start, col("w").end)

    # Datastream RFI flagging function, key by baseline ID
    ds_rfi_qa = ds_flagged.key_by(lambda row: row[1]) \
        .flat_map(RfiQaRuns(), output_type=Types.ROW([Types.INT(), Types.INT(), Types.STRING()]))

    table_rfi_qa = t_env.from_data_stream(ds_rfi_qa,
                                          Schema.new_builder()
                                          .column("f0", DataTypes.INT())
                                          .column("f1", DataTypes.INT())
                                          .column_by_expression("ts", "CAST(f2 AS TIMESTAMP(3))")
                                          .build()
                                          ).alias("baselineId, nRfiSamples, RfiEndTime")

    # Datastream which records the current flag in the baseline
    ds_rfi_qa_current = ds_flagged.key_by(lambda row: row[1]) \
        .flat_map(RfiQaCurrent(), output_type=Types.ROW([Types.INT(), Types.INT()]))
    # Convert the datastream to a table so it can be written using table sinks.
    table_rfi_qa_current = t_env.from_data_stream(ds_rfi_qa_current,
                                                  Schema.new_builder()
                                                  .column("f0", DataTypes.INT())
                                                  .column("f1", DataTypes.INT())
                                                  .build()
                                                  ).alias("baselineId, nRfiFlag")

    # To have multiple sinks in a job we use statement sets
    # create a statement set
    statement_set = t_env.create_statement_set()
    statement_set.add_insert("es_summary_sink", table_flagged)
    statement_set.add_insert("es_rfi_run_sink", table_rfi_qa)
    statement_set.add_insert("es_rfi_current_sink", table_rfi_qa_current)
    statement_set.execute()

    # Prints the execution plan for illustrative purposes, remove the -d option to see the output.
    # The output can be visualised using steps here:
    # https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/execution/execution_plans/
    print(env.get_execution_plan())


if __name__ == '__main__':
    qa_processing()
