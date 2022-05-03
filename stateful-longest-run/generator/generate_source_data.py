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
from kafka import KafkaProducer
from kafka import errors 
from json import dumps
from time import sleep


def write_data(producer):
    data_cnt = 2000000
    order_id = calendar.timegm(time.gmtime())
    max_price = 100000
    topic = "random_binary_msg"

    for i in range(data_cnt):
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        random_binary = randint(0, 1)
        cur_data = {"createTime": ts, "binaryInt": random_binary}
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
