# airflow


Apache Airflow is an open-source workflow management platform for data engineering pipelines. It's integrated wwith many other services among them aws.

The principle component here is called a Dag. A Dag is a set of  ordered tasks. 

In this project we built a pipeline using three tasks. The first task consists of gathering data about apple stock (using apple api), the second task consists of creating the bucket in s3 if it not exits. And at last we send the data we collect to aws-s3 without storing it on any local disk.
