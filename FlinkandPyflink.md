# Flink #

Flink is a framework for [distributed](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/overview/) stream (and batch) processing. You interact with Flink through one of its API's.

Flink has two main API's the [**Table API**](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/table/intro_to_table_api/) and the [**Datastream API**](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/datastream/intro_to_datastream_api/). Their relationship is illustrated in Fig. 1 below, taken from [here](https://www.youtube.com/watch?v=vLLn5PxF2Lw).

![alt text](images/Flink%20Architechture.PNG)

A Flink job requires two things, a **source** (an entry point for streaming/batch data) and a **sink** (an exit point for the streaming /batch data).

Between the source and the sink you probably want some more or less non-trivial **transformation on your data**.
To accomplish this most Flink programs follow the flow:
1. Obtain an execution environment,
2. Load/create the initial data, (**source**)
3. Specify transformations on this data, 
4. Specify where to put the results of your computations, (**sink**)
5. Trigger the program execution

Step 2. is where you use an API to specify the desired transformation. 

Flink features a graphical dashboard which can be used to see and manage running and completed jobs.

Flink is written in **Scala** and **Java**, however jobs can also be submitted using **Python**. This is thanks to the Python API ([**pyflink**](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/overview/)).
### Python + Flink = Pyflink ##
Using pyflink you can submit python code to be executed as Flink jobs. Pyflink supports both the Datastream and Table API's, simple examples of using pyflink can be found [here](https://github.com/apache/flink/tree/release-1.14/flink-python/pyflink/examples).

Pyflink's **Table API**  allows users to create and utilise **User Defined Functions ([UDF's](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/table/udfs/overview/))**, allowing you to use python, including python libraries such as pandas, to perform operations on the data.

### A Tale of Two APIs: ###
In the **Table API** the data is abstracted as a [dynamic table](https://nightlies.apache.org/flink/flink-docs-release-stable/docs/dev/table/concepts/dynamic_tables/#dynamic-tables-amp-continuous-queries) (the Pyflink `TableEnvironment` handles this, it is used for creating tables, submitting jobs etc. ) which you can manipilate as you would a table. Using python dynamic tables can be manipulated in the same way as a pandas table. The relational planner and rest of the Flink stack takes care of translating and optimising the operations described on the table abstraction into streaming or batch operations, including managing state when required. For example in summation operations (an aggregate operation) etc. *Sources and sinks can be defined using either **SQL** or **python***

The **Datastream API** allows you more control over how the programme is executed, it does not feature the table abstraction. In the DatastreamAPI the ``StreamExecutionEnvironment`` is what handles the abstraction, it is used to create datastreams and execute jobs etc. However, this API does not appear to support manipulation using Pandas etc. since it does not abstract the data as a table. 

It is possible to [translate between](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/datastream/intro_to_datastream_api/#conversion-between-datastream-and-table) the different APIs and you may therefore combine them in a symbiotic fashion.

## I want my job to run in parallel and quickly* (etc.)!
For your flink job to execute in parallel there need to be several task slots, this is so Flink can allocate tasks to be performed in parallel to different slots. If the concept of task slots is new to you, I would strongly encourage you to read the short text below.

### Task Managers and Task Slots         
Task Managers, are the worker processes in Flink [[1]](https://learning.oreilly.com/library/view/stream-processing-with/9781491974285/ch03.html#chap-3-setup-components), if you use docker they run in different containers, perhaps with fun names such as *"example-pipeline_taskmanager_16"*. ***Task managers* are managed by the *Job Manager***, which allocates them work. 

Each Task Manager can have one or several Task Slots, each Task Slot can be used to perform some task. The Task Slots in one job manager can perform tasks which are a part of the same job, or different jobs, any tasks the Job manager allocates them. Tasks are run in separate threads within the same Java Virtual Machine (JVM) on the Task Manager. There is therefore less communication overhead for tasks executing in slots on the same Task Manager, but on the flipside a failure in one task can kill the entire Task Manager and all its tasks (the Job Manager takes this into account when assigning work). Furthermore, if a task Manager Crashes the Job Manager will, depending on restart strategy, reallocate those tasks to other available task slots if there are any.

### Ok great, but how do I run things in parallel?
Thank you for reading the above text on Task Slots. You can specify the parallelism you want for the entire job, and/or individual operations to be performed with. In Python you can set the parallelism of the environment in a job with ``env.set_parallelism(n)``, where *n* is the parallelism (if nothing is specified the default is a parallelism of 1). Flink then tries to allocate work to different Task Slots based on this, so for a parallelism of *n* you need at least *n* available Task Slots. 

That is it, just tell Flink how much parallelism you want and give it the task slots and Flink takes care of the rest.

**At the moment I cannot tell you how to make your flink job run quickly.*

## So what happens when I submit a job?
When you submit a job you provide a high level plan of what you want Flink to do. Flink's **Job Manager** then takes a look at your instructions and another look at what it has to work with (for example, taskmanagers, taskslots etc.) and forms an *execution plan*. This execution plan then determines the specifics of how flink executes your job, the execution plan for a job can extracted and visualised as shown [here](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/execution/execution_plans/).


## What now?
With this birds eye overview of Flink I would suggest looking at the examples, these include:
* The *word count example*, loads data from storage and counts the number of occurrences of each word, writes output to standard out.
* The *docker word count example* to see how docker can be used to produce the same result.
* The *pyflink-demo1* example, this is a more involved example featuring a metric generator connected to a kafka message broker which generate continous data. This data is then processed and stored using elasticseach, it can be visualised with Kibana.

When implementing custom stateful functions you have to be somewhat careful, this is since to allow Flink to handle states cleverly you have to declare states, and states are tied to keys in the data (so they may be backed up etc.). The more involved example will feature a stateful function which combines both API's.
[This example is illuminating for working with states](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/).

### Issues encountered
1. **Kafka/ES sinks and sources: connector issues due to changed interfaces in Flink 1.14**. Example 2 originally used Flink 1.13 and the connectors needed to interface with kafka and elasticsearch caused much trouble when upgrading to Flink 1.14. Given the evidence scattered online the following sequence of events seem to have cause the problem: in Flink 1.12 [a new kafka interface was introduced](https://nightlies.apache.org/flink/flink-docs-release-1.12/release-notes/flink-1.12.html) for connectors, and in [Flink 1.14](https://nightlies.apache.org/flink/flink-docs-release-1.14/release-notes/flink-1.14/) they [discontinued the old format and changed all connector interfaces](https://issues.apache.org/jira/browse/FLINK-23513). Thus: if you try to use connectors but get an error along the lines of `pyflink.util.exceptions.TableException: org.apache.flink.table.api.TableException: findAndCreateTableSource failed.` this may be what is causing you grief.

2. **If the job suddenly fails as the parallelism increases**, a cause may be that some watermarks are not used. Thus parts of the execution halts indefinitely waiting for the unused watermark, this does not throw an error and can be difficult to troubleshoot.
