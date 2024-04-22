# Distributed Messaging Queue
This is a distributed messaging queue where we have three components -
- Brokers
- Producer
- Consumer

## Overview

### Brokers
- The code for brokers is in the file `server.py`. The brokers are responsible for taking messages from the producers and storing them to be consumed later by consumers.
- Each instance of broker maintains all the information in its own database (SQLITE). The database among all the instances of brokers are synced.

### Consumer
- The conde for consumers is present in `myconsumer.py`.
- The consumer must be provided an instance of `Properties` object. This object must contain the following -
  - local of zookeeper
  - consumer id
  - the topic 
- The consumer needs to register itself with the broker once.
- The consumer has method which will contact the broker and fetch the messages from the topic mentioned.

### Producers

- The code for the producer is present in `myproducer.py`.
- The producer takes an instance of `Properties` object. This object contains required configurations -
  - address of zookeeper
  - producer id
- The producer needs to register itself with the broker once.
- To send a message to the broker, the producer has send method which takes an instance of `ProducerRecord` as input. This instance has the following information -
  - the destination topic.
  - the message content.

## How to run

For the purpose of demo, we have created a simple Produce-Consume-Transform pipeline. Inside the `demo` directory we have two files -
- InvoiceGen.py: This generates invoices. Some invoices are valid invoices whereas some are invalid. ALl the invoices are sent to `All Invoices` topic.
- InvoiceFilter.py: This consumes the invoices from `All Invoices` topic and sends the valid invoices to `Valid Invoices
 topic and invalid invoices to `Invalid Invoices` topic.
### Zookeeper
- The first step is to run zookeeper. The script for running zookeeper is present in `scripts` directory.
- The script must be modified with the location of confluent directory.

#### Zookeeper logs

- For convenience, our terminal runs inside root directory of the project - `Messaging Queue`.
- We have configured `zookeeper.properties` in such a way that all the zookeeper logs gets generated inside the folder `generated_logs` present in the root directory of the project.

### Database files

- The database files gets generated inside the folder `db`. Whenever the project needs to be restarted, the following things need to be done - 
  - delete all the files in `db` directory.
  - delete zookeeper logs from `generated_logs` folder.

### Running brokers

- Run server.py with command line argument as the port number on which the server should run. Ex - `python3 server.py 5001`.
- Run as many instances as you wish on different port numbers.

### Running InvoiceGen.py
- This takes three arguments which are to be supplied from the command line - 
  - generator name
  - location of zookeeper 
  - id of the generator
- Ex - `python3 InvoiceGen.py gen1 localhost:2181 1234`
- Run as many instances as required with unique name and id.

### Running InvoiceFilter.py
- This takes two arguments which are to be supplied from the command line - 
  - filter name
  - location of zookeeper
- Ex - `python3 InvoiceFilter.py filter1 localhost:2181`
- Run as many instances as required with unique name.



