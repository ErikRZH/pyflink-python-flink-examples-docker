## Wordcount examples
This is the example from [here](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/table_api_tutorial/) and [here](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/datastream_tutorial/) respectively running in docker container.

First build the pyflink image
```
sudo docker pull flink:1.14.4-scala_2.11
sudo docker build --tag pyflink:1.14.4-scala_2.11 .
```

docker-compose (add sudo according to your preferences).
````
sudo docker-compose up -d
````

To the examples correspond to using the Table API in batch (``wordcount_table_batch.py``) and stream modes (``wordcount_table_streamin.py``), as well as the same for the Datastream API (``wordcount_´ds_batch.py``, ``wordcount_ds_streaming.py``).

To run these jobs use 
````
sudo docker-compose exec jobmanager ./bin/flink run --detached -py /opt/wordcount/word_count_ds_streaming.py -d
````

With the job you want to run.

The outputs are written to stdout.

To get information about the job, and manage jobs you can consult the web UI at `http://localhost:8081/`.
Find stdoutdocker compose up -d in the web UI by clicking the job -> clicking a name on the bottom -> choose the "TaskManagers" tab on the right and click "LOG" -> stdout.

To shut down the containers:
```
sudo docker-compose down
```