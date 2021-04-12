import pandas as pd

import json
import logging
import os
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

QUEUE_NAME = os.getenv("Queue")
SQS = boto3.client("sqs")

def getQueueURL():
    """Retrieve the URL for the configured queue name"""
    q = SQS.get_queue_url(QueueName=QUEUE_NAME).get('QueueUrl')
    return q


def lambda_handler(event, context):
    try:
        u = getQueueURL()
        logger.debug("Got queue URL %s", u)
        for record in event['Records']:
            filename = record['s3']['object']['key']
            bucket = record['s3']['bucket']['name']
            s3path = "s3://"+bucket+"/"+filename
            logger.debug("s3 path {}".format(s3path))
            resp = SQS.send_message(QueueUrl=u, MessageBody=s3path)
            logger.info("Send result: %s", resp)

    except Exception as e:
        raise Exception("Could not record link! %s" % e)


df = pd.read_csv("01-thewilliamcartercartersus_20210329-100000.tsv.zip",sep="\t")

print(df.head(10))

#s3path ="s3://airflow-emr-smh/carters/clickstream/tab/"
#s3path_array = s3path.split("//")[1]
#index = s3path_array.find("/")
#bucket = s3path_array[0:index]
#folder =s3path_array[index+1:len(s3path_array)]

