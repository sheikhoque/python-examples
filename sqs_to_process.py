import boto3
import logging
import argparse
import time
import s3_to_kinesis

sqs = boto3.client("sqs")


def get_queue_url(q_name):
    """Retrieve the URL for the configured queue name"""
    q = sqs.get_queue_url(QueueName=q_name).get('QueueUrl')
    return q


def receive_message(q_url):
    response = sqs.receive_message(
        QueueUrl=q_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,
    )

    logging.info(f"Number of messages received: {len(response.get('Messages', []))}")
    for message in response.get("Messages", []):
        return message["Body"], message['ReceiptHandle']


def delete_message(q_url, receipt_handle):
    response = sqs.delete_message(
        QueueUrl=q_url,
        ReceiptHandle=receipt_handle,
    )
    print(response)


def main(args):
    queue_name = args.queue_name
    config_table = args.config
    meta_table = args.metastore
    service_type = args.service
    queue_url = get_queue_url(queue_name)
    while True:
        (file_path, receipt_handle) = receive_message(queue_url)
        s3_to_kinesis.run_process(config_table, meta_table, service_type,
                                  "s3://airflow-emr-smh/carters/clickstream/tab/test3.tsv")
        delete_message(queue_url, receipt_handle)
        time.sleep(60)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='sqs_to_process_triggers',
                                     description='Reads the file name from SQS and trigger python process to read the '
                                                 'file and send to kinesis')

    parser.add_argument('--config', action="store", type=str, default="carters_clickstream_config",
                        help="The name of config table in dynamo")
    parser.add_argument('--metastore', action="store", type=str, default="carters_streaming_metadata",
                        help="The name of metastore table in dynamo")
    parser.add_argument('--queue_name', action="store", type=str, default="carters_sqs",
                        help="sqs queue name")
    parser.add_argument('--service', action="store", type=str,
                        default="lambda",
                        help="configuration for service")

    main(parser.parse_args())
