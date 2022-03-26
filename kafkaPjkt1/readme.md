- Start the Docker Container and Open a New Shell Inside
- Start Kafka
- Install SQLite3 and Create a New Database Table
- Insert at Least Two New Values into Your New Database Table
- Start Kafka Connect in Standalone Mode
- Verify the Connectors and Topic Have Been Created
- Start a Kafka Consumer and Write New Data to the Database



### Start the Docker Container and Open a New Shell Inside

Start a docker container
`docker run -it <image_name>`

The reference to docker run is [here](https://docs.docker.com/engine/reference/run/)


`docker run -ti --rm --name sqlite-demo --network host confluentinc/docker-demo-base:3.3.0`

`--name` to specify a container name
`--network` to create a network, all the containers in the network can communicate among themselves


if you get an error, here are some docker commands that can help by debbugging:
- `docker container ls`
- `docker container stop <container name>`
- `docker container rm <container name>`

#### Stat confluent kafka

cd /tmp
confluent start

#### install SQLite3 

apt-get update
apt-get install sqlite3

#### create a new database

sqlite3 test.db

#### crete a new Table

CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, name VARCHAR(255));

mind the semicolon at the end of the command

Verify the table has been create with:

`sqlite> .tables`

#### Insert values in the table


```
INSERT INTO accounts (name) VALUES ('chad');
INSERT INTO accounts (name) VALUES ('terry');
```

Verify that the records have been inserted in the table **accounts**:

`SELECT * FROM accounts`

with the above query all existing record are shown.




#### Troubleshooting

Check the log, especially useful if something went wrong

`cat /logs/connectStandalone.out`

An error I had, may be because I run some the confluent platform multiple times, is the following (copied from the log):

```
WARN FAILED ServerConnector@65b863b9{HTTP/1.1}{0.0.0.0:8083}: java.net.BindException: Address already in use (org.eclipse.jetty.util.component.AbstractLifeCycle:212)
java.net.BindException: Address already in use
```
