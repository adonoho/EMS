# Connecting to the Project Database:

Exactitude keeps all of its working data in a 24x7 PostgreSQL database hosted in the cloud. While currently we are using Google's cloud and these instructions are specific to them, it is very likely that each cloud provider will have some variant of the same basic pattern. Why? Because the internet is a dangerous place and databases are frequent targets of hacker attacks. Hence, the cloud vendors enforce secure practices on their managed database products. As we want our scientific data to be robust and reproducible, we have a similar set of security requirements to any other enterprise.

## Why PostgreSQL?

We have chosen a mainstream database instead of a cloud service, such as Google's Big Query or AWS's DynamoDB. Why? It can be flexibly deployed to almost every cloud service and, if it becomes too expensive to run in the cloud, we can move it to our campus computing infrastructure. The major tools, Python, R, and Matlab, can each communicate with SQL databases. There are single line calls for reading and writing whole tables of data in each environment. Support for vendor specific cloud/noSQL databases is uneven. Finally, the JupyterHub lab software utilizes PostgreSQL also. Hence, as we move to do more of our computations in Jupyter notebooks, there will be some shared knowledge for the system administrators. This is a _"good thing."_

## Why Google Cloud SQL?

We are using Google's Cloud SQL service to host our PostgreSQL database. This gives us a really nice service where Google performs all database management, including backups and replication. These are tedious to manage and, hence, most research organizations fail to perform these basic robust data management practices. It is much better to hire Google, AWS, or others to do these mundane services. This allows research staff to focus upon the task at hand. (Considering that team members have lost data from a too enthusiastic management of compute nodes, our solution is to get the data out of the compute node as quickly as is practical.) Finally, databases are frequently the target of hackers and other _n'er do wells_. If we want to avail ourselves of a database's virtues, then we need to mitigate its challenges. By using Google's Cloud SQL managed service, we gain the benefit of their state of the art security along with a standardized pattern of access, called the Cloud SQL Proxy.

## Why a Proxy Service?

Google has code that allows Python to directly access the Cloud SQL database. Hence, why use a proxy at all? Because there are third party tools, such as Tableau and R Studio, which do not have the details about Google's Cloud SQL service compiled into them. Yet, we want to use these important tools to get access to the data. Google's answer is to provide a proxy service that you set up on your laptop. This service will handle all of the security negotiations with Google and gives each of these 3rd party apps a simpler interface to the Cloud SQL database. This means that Google is responsible for security and the third parties have an interface that behaves the same as if the database was local. This is, as they say, a win-win.

## Deployment Plan:

We are going to do two things:
- Install the Cloud SQL Proxy.
- Make R Studio Use It.

### Install the Cloud SQL Proxy.

Google's full instructions are [here](https://cloud.google.com/sql/docs/postgres/connect-admin-proxy). We assume that you already have an account on the Google Cloud Platform associated with the research group's account, `hs-deep-lab-donoho`. 