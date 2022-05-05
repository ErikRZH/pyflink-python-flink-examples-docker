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
import time, calendar
from random import randint
import math
from kafka import KafkaProducer
from kafka import errors 
from json import dumps
from time import sleep

random.seed(137)

def write_data(producer):
    data_cnt = 2000000
    topic = "baseline_signal"
    # Generate sine wave and random data interspersed
    for i in range(data_cnt):
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        if int(i//60)%2 == 0: # Returns 0 if i<60 and 1 if i>60 and so on alternating.
            signal_value = random.random()*2 - 1
        else:
            signal_value = math.sin((i/data_cnt)*1e5)

        baseline_id = random.choice([42,22,5])
        cur_data = {"createTime": ts, "baselineInd": baseline_id, "signalValue": signal_value}
        producer.send(topic, value=cur_data)
        sleep(0.5)

def create_producer():
    print("Connecting to Kafka brokers")
    for i in range(0, 6):
        try:
            producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                            value_serializer=lambda x: dumps(x).encode('utf-8'))
            print("Connected to Kafka")
            return producer
        except errors.NoBrokersAvailable:
            print("Waiting for brokers to become available")
            sleep(10)

    raise RuntimeError("Failed to connect to brokers within 60 seconds")

if __name__ == '__main__':
    producer = create_producer()
    write_data(producer)
