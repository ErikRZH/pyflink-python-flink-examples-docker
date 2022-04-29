## Stateful Function using Both APIs II

This example uses both API's and a stateful streaming API function.

The example consists of:  
(1) Messages with a randomly selected 1 or 0 and a timestamp are generated and sent using kafka.  
(2) Using Flink the longest run seen until this point is calculated, and when it is found it is notes, along with a timestamp and whether it was 0's or 1's.
(3) The processed data is stored using elasticsearch and may be visualised with Kibana. So one can check the distribution of the computed average.

The Flink part of the programme follows the flow:
* Sink read from Kafka into Table [Table API]
* Datastream formed from table [Table API, Datastream API]
* A stateful function is applied to each "Row" of the datastream (corresponding to table rows), this outputs an average integer and the timestamp of the second integer. [Datastream API, Custom stateful function]
* A Table is formed from the transformed datastream [Datastream API, Table API]
* Resulting table is written to the Elasticsearch sink. [Table API]

To start the example, build the images:
````commandline
sudo docker-compose build
````

Then start the containers
````
sudo docker-compose up -d
````

To see the payment messages being sent you can run:
````
sudo docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic random_binary_msg
````
To see the elasticsearch data (before the job there should be none) look at [http://localhost:9200/platform_run_1/_search?pretty](http://localhost:9200/platform_run_1/_search?pretty).

Submit the payment processing job
````commandline
sudo docker-compose exec jobmanager ./bin/flink run -py /opt/stateful-longest-run/demo_longest_run.py -d
````

You can inspect the job at the various stages at:  
Flink Web UI [http://localhost:8081](http://localhost:8081).   
Elasticsearch [http://localhost:9200](http://localhost:9200).   
Kibana [http://localhost:5601](http://localhost:5601).

On Kibana there is a dashboard showing the spending of the different teams.

To shut it down.
```
sudo docker-compose down
```