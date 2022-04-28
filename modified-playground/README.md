## Modified Playground

**This example is the one [here](https://github.com/pyflink/playgrounds) but modified to work with Flink 14.4 and with minor cosmetic changes.**
See the original repository for more details.

The example consists of (1) messages with a team_id and an amount are generated and sent using kafka.
(2) Using Flink the team_id is replaced with the team name and the sum of all payments is calculated. (3) The processed data is stored using elasticsearch and may be visualised with Kibana.

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
sudo docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic payment_msg
````
To see the elasticsearch data (before the job there should be none) look at [http://localhost:9200/platform_pay_amount_1/_search?pretty](http://localhost:9200/platform_pay_amount_1/_search?pretty).

Submit the payment processing job
````commandline
sudo docker-compose exec jobmanager ./bin/flink run -py /opt/modified-playground/payment_msg_proccessing.py -d
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