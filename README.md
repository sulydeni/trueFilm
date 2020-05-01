# True Film Challenge

### Tech

This Project is based on Amazon linux 2 OS (also comaptible with Centos 7) and is mainly written in Python3.

This project is dependent on:

* [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) - Apache Spark is an open-source cluster-computing framework, built around speed, ease of use, and streaming analytics whereas [Python](https://www.python.org/) is a general-purpose, high-level programming language that can interact with Spark framework through pyspark.
* [Docker](https://www.docker.com/) - Docker is a tool designed to make it easier to create, deploy, and run applications by using containers.
* [Postgresql](https://www.postgresql.org/) - An RDBMS database and was required in the project brief.
* [Jupyter](https://jupyter.org/) - The Jupyter Notebook is an open-source web application that allows you to create and share documents that contain live code, equations, visualizations and narrative text.

#####  Challenges and Choices

 - I could have written the whole code in python but that would require a much longer script to efficiently consume multiple cores and memory on the instance. 
 **Choices:**
    - I have chosen to run my script on the top of Apache Spark framework as it uses the full multiple VCPU efficiently and it fully supports reading large structured xmls through [spark-xml](https://github.com/databricks/spark-xml).
    - Spark dataframe supports numerical, long list of functions and string regex operations on columns.

- Pyspark and Postgres require a long setup process that is OS type and version dependant.  I have used [official postgres docker container](https://hub.docker.com/_/postgres) and for Pyspark I have built a reproducable docker from my amazon linux 2 setup process.
 **Choices:**
    - I have pulled the docker container for an OS independent reliable deployment and all shell scripts are minimal to make the project transferrable to other OSs.
 

### Installation

The project requires Amazon Linux 2 OS to run.

Installing the dependencies, download the main files then run the pyspark scripts. The building and running processes are packaged in one bash script "build_and_run.sh".

```sh
$ cd trueFilm
$ sh build_and_run.sh
```
##### Benchmark
When tested on m5.2xlarge( 8 VCPU and 32 GB (RAM)) - The process takes "13m 29.814s"

##### Testing

For testing and developing run the build_and_run.sh script and add variable test. This will run a jupyter notebook where you can test and develop on main.ipynb script. 

```sh
$ sh build_and_run.sh test
```

To check the data for correctness, I have been using test driven approach where I was downloading (partial random or complete) samples of my data as CSV and examining trends through excel in my local computer environemnt.

*For more information about approach details and algorithm choices please refer to the main.ipynb annotations.*