# ML-IDS API Client

## General

This project implements Python clients which can be used to interact with the server components of the [ML-IDS project](https://github.com/cstub/ml-ids).    
The ML-IDS project is an implementation of a machine learning based [intrusion detection system](https://en.wikipedia.org/wiki/Intrusion_detection_system), providing a classifier capable of detecting network attacks. The classifier analyses prerecorded network flows and categorises a network flow as either benign or malicious. A network flow in this context is defined as an aggregation of interrelated network packets between two hosts.

The classifier is made accessible by the [ML-IDS API project](https://github.com/cstub/ml-ids-api) which implements a REST API to serve the classifier.
This REST API accepts prerecorded network flows and returns predictions classifying the network flows as either benign or malicious.   
Once a malicious network flow has been detected by the ML-IDS API, an attack notification is sent to an [AWS SQS queue](https://aws.amazon.com/sqs/) which clients can subscribe to in order to be notified of detected attacks.    
For further details concerning the REST API please consult the [ML-IDS API documentation](https://github.com/cstub/ml-ids-api).

The ML-IDS API can be used by issuing a corresponding REST request, submitting the  network flows to be categorized in the request body. To receive attack notifications issued by the ML-IDS API, a client subscribing to the AWS SQS attack notification queue is required.    
This project implements two ready-to-use Python clients handling these use-cases. The clients can be used to issue new classification requests via the ML-IDS REST API and to receive attack notifications via the corresponding AWS SQS queue. The purpose of these clients is to provide an easy to use command-line interface for network flow classification and notification receipt.

Network flows can either be taken from the provided test dataset hosted on AWS S3 (s3://ml-ids-2018-full/testing/test.h5) or be created by recording network traffic via [Wireshark](https://www.wireshark.org/) and converting the recorded traffic data into network flows using [CICFlowmeter](http://netflowmeter.ca/).

## Installation

To use the clients, checkout the project and create a new Anaconda environment from the `environment.yml` file.

```
conda env create -f environment.yml
```

Afterwards activate the environment and install the project dependencies.

```
conda activate ml-ids-api-client

pip install -e .
```

After successful installation the command-line clients `ml_ids_rest_client` and `ml_ids_attack_consumer` are available in the current environment.

## ML-IDS REST Client

The ML-IDS REST client offers a command-line interface to issue prediction requests to the ML-IDS REST API. The network flows to create the prediction requests are supplied via a dataset stored in HDF format.
The client accepts datasets stored on the local file-system or hosted on an AWS S3 bucket.   

To use the client, the URL of the prediction API endpoint and the path to the dataset has to be provided. In absence of a custom dataset you can use the provided dataset hosted on AWS S3, which represents a subset of the [CSE-CIC-IDS2018](https://www.unb.ca/cic/datasets/ids-2018.html) dataset.

```
ml_ids_rest_client \
  --api-url PREDICTION_ENDPOINT_URL \
  --dataset-uri s3://ml-ids-2018-full/testing/test.h5
```

Upon start of the client you can select one or more network flows from the dataset. Network flows can either be selected by category (e.g. Benign, DoS attack, Bot, ...) or chosen randomly.    
Following the selection of the traffic category, the number of network flows which should be sent in a single prediction request can be selected. Afterwards the prediction request can be submitted or a send-delay may be specified.        
By defining a send-delay, a prediction request containing multiple network flows is split into multiple requests, each containing a single network flow. Sending of consecutive requests is delayed by the period specified.    
Upon receipt of the API responses, the client processes the responses and combines the received predictions with the original network flow data, to determine if the prediction was correct. The results are displayed afterwards.

## ML-IDS Attack Consumer

The ML-IDS attack consumer represents a simple command-line client that can be used to subscribe to an AWS SQS queue containing attack notifications published by the ML-IDS API.    
Upon start of the client the specified SQS queue is subscribed to and periodically polled for new attack notifications. Once an attack notification is received the details of the corresponding network flow are displayed.    
Notifications can either be deleted after receipt or be preserved in the queue.

To use the client, the URL and the region of the AWS SQS queue has to be provided, combined with valid AWS credentials having permission to read messages from the SQS queue.

```
ml_ids_attack_consumer \
  --queue-url AWS_SQS_QUEUE_URL \
  --region AWS_REGION \
  --access-key AWS_ACCESS_KEY \
  --secret-key AWS_SECRET_KEY \
  --delete-messages True
```
