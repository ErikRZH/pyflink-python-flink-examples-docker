## Signal Quality Assessment (QA) Pipeline Example

This example is meant as a prototype to show how a distributed streaming pipeline for quality assessment of radio astronomy signals may be implemented using Flink. This consists of generating simplified signals from baselines (elements of a radio telescope). and then flagging the signal if it contains radio-frequency interference (RFI). Some metrics which may be used to gauge the signal quality and functioning of the fictional radio telescope are calculated from the data. These metrics are written to elasticsearch and can be seen on a Kibana dashboard.


### Overview of Pipeline
The pipeline (shown pictorially in ***Fig.1***) consist of the following steps:

1. A stream of mock receive data, consisting of mock visibilities (floats) from spoof baselines is received via Kafka. 
2. The input stream is split depending on the baseline, this is to allow the QA metric calculation and spoof RFI flagging to be distributed and parallelised for different baselines.
3. A spoof RFI flagger generates flags for each visibility.
4. This flagged stream is again split so three different QA Metrics can be calculated independently.
5. The calculated QA metrics are saved in an Elasticsearch database.



![alt text](images/pipeline_overview.png)
***Fig. 1** Overview of the quality assessment pipeline in this example.*

## Running the Job

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
sudo docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic baseline_signal
````
To see the elasticsearch data for the different QA metrics (before the job there should be none) look at:         
[http://localhost:9200/example_pipeline_summary_1/_search?pretty&size=50](http://localhost:9200/example_pipeline_summary_1/_search?pretty&size=50),           
[http://localhost:9200/example_pipeline_rfi_run_1/_search?pretty&size=50](http://localhost:9200/example_pipeline_rfi_run_1/_search?pretty&size=50),        
[http://localhost:9200/example_pipeline_rfi_current_1/_search?pretty&size=50](http://localhost:9200/example_pipeline_rfi_current_1/_search?pretty&size=50)

Since the Flink container is now running you should see that you have one task manager ([covered here](../FlinkandPyflink.md)) with two task slots and no jobs running. If you open Flink Web UI [http://localhost:8081](http://localhost:8081) it should look as in ***Fig.2***

![alt text](images/flink_on_start.PNG)
***Fig. 2** Flink Web UI on startup.*

**The job has a specified parallelism of 5 and therefore requires at least 5 task slots.**
Launch more task managers (each with two slots in this case) to get 5 task slots or more. To launch more task managers, so the total is ```<N>``` use.

```
sudo docker-compose scale taskmanager=<N>
```
The Flink Web Ui should show the new number of task managers and task slots.

Submit the job using
````commandline
sudo docker-compose exec jobmanager ./bin/flink run -py /opt/example-pipeline/example_qa_job.py -d
````

The job should now be shown as running in the UI and if you click the job name you will get information regarding the job, as in ***Fig. 3***.

![alt text](images/flink_running_job.PNG)
***Fig. 3** Flink Web UI showing the job*

The Elasticsearch [http://localhost:9200](http://localhost:9200) database should now also contain records, which you can check using the URL's above.

**You can now look at the Kibana Dashboard showing the QA metrics generated**. Going to Kibana [http://localhost:5601](http://localhost:5601), and navigating to **Dashboards in the sidebar** and then selecting the *"Signal Quality Assessment Prototype Dashboard"* should bring up a page showing the live metrics (click refresh to update them), the dashboard should appear as in ***Fig.4***.

![alt text](images/kibana_dashboard.PNG)
***Fig. 4** Kibana Dashboard showing metrics calculated by the Flink job.*

On Kibana there is a dashboard showing the different QA metrics.

To shut it down.
```
sudo docker-compose down
```

## When the Job is Running

###Testing checkpointing
To check that the checkpointing and state recovery system works correctly you can kill a taskmanager which is being used and see how Flink deals with this.
To find a task manager in use, click one of the processes in the UI, either by clicking the blue box, or by selecting its name. Then navigate to the **Task Managers** tab, you should see what task managers execute this process, as in ***Fig. 5***.

![alt text](images/flink_task_managers.PNG)
***Fig. 5** List of task managers in use by this process.*
In ***Fig. 5*** the task manager ``example-pipeline_taskmanager_4`` is in use, we will see what happens if this stops. 

However first look at the **Checkpoints** tab of the job, this shows all the checkpoints of the state that the job manager keeps (checkpoint strategy is specified in the job). The **Checkpoints**tab should appear as in ***Fig. 6***

![alt text](images/flink_checkpoints_highlighted.PNG)
***Fig. 5** The latest restored checkpoint is highlighted, in this case no error has occurred so the state has not been restored from a checkpoint.*

If you then kill ``example-pipeline_taskmanager_4`` using:
```
sudo docker kill example-pipeline_taskmanager_4
```

The job will then predictably fail. When the job manager notices the worker has died (so number of workers decreases in the Web UI), it will utilise other task slots and restore the state from a checkpoint, so the "Latest Restore" will update to reflect this, and no data should be lost (can be confirmed by checking the records in Elasticsearch). 

This ability to have ***at least once*** consistency with quick stateful streaming processing, specifying only the job and the job manager taking care of the rest is one of the main appeals of FLik (to the best of my understanding).

###Considerations
If the job suddenly fails as the parallelism increases, a cause may be that some watermarks are not used. Thus parts of the execution halts indefinitely waiting for the unused watermark, this does not throw an error and can be difficult to troubleshoot.
