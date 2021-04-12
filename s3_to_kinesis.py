import boto3
from botocore.exceptions import ClientError
import datetime
import pandas as pd
import json
import logging
import argparse
import io
import zipfile
import gzip

dynamo = boto3.resource('dynamodb')
s3 = boto3.resource('s3')
kinesis = boto3.client('kinesis')


class S3File(io.RawIOBase):
    def __init__(self, s3_object):
        self.s3_object = s3_object
        self.position = 0

    def __repr__(self):
        return "<%s s3_object=%r>" % (type(self).__name__, self.s3_object)

    @property
    def size(self):
        return self.s3_object.content_length

    def tell(self):
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = self.size + offset
        else:
            raise ValueError("invalid whence (%r, should be %d, %d, %d)" % (
                whence, io.SEEK_SET, io.SEEK_CUR, io.SEEK_END
            ))

        return self.position

    def seekable(self):
        return True

    def read(self, size=-1):
        if size == -1:
            # Read to the end of the file
            range_header = "bytes=%d-" % self.position
            self.seek(offset=0, whence=io.SEEK_END)
        else:
            new_position = self.position + size

            # If we're going to read beyond the end of the object, return
            # the entire object.
            if new_position >= self.size:
                return self.read()

            range_header = "bytes=%d-%d" % (self.position, new_position - 1)
            self.seek(offset=size, whence=io.SEEK_CUR)

        return self.s3_object.get(Range=range_header)["Body"].read()

    def readable(self):
        return True


# function for sending data to Kinesis at the absolute maximum throughput
def send_to_kinesis(kinesis_stream_name, data):
    kinesis_records = []  # empty list to store data
    (rows, columns) = data.shape  # get rows and columns off provided data
    current_bytes = 0  # counter for bytes
    row_count = 0  # as we start with the first
    total_row_count = rows  # using our rows variable we got earlier
    send_kinesis = False  # flag to update when it's time to send data

    for i in data.index:
        value = json.dumps(data.loc[i].to_json())
        encoded_values = bytes(value, 'utf-8')  # encode the string to bytes
        # create a dict object of the row
        kinesis_record = {
            "Data": encoded_values,  # data byte-encoded
            "PartitionKey": str(i)  # some key used to tell Kinesis which shard to use
        }
        kinesis_records.append(kinesis_record)  # add the object to the list
        string_bytes = len(value.encode('utf-8'))  # get the number of bytes from the string
        current_bytes = current_bytes + string_bytes  # keep a running total
        # check conditional whether ready to send
        if len(kinesis_records) == 500:  # if we have 500 records packed up, then proceed
            send_kinesis = True  # set the flag
        if current_bytes > 50000:  # if the byte size is over 50000, proceed
            send_kinesis = True  # set the flag
        if row_count == total_row_count - 1:  # if we've reached the last record in the results
            send_kinesis = True  # set the flag
        # if the flag is set
        if send_kinesis:
            # put the records to kinesis
            try:
                response = kinesis.put_records(
                    Records=kinesis_records,
                    StreamName=kinesis_stream_name
                )
            except ClientError as e:
                logging.error(
                    "Error while trying to send data to {}, error message:{}".format(kinesis_stream_name,
                                                                                     e.response['Error']['Message']))
                return None

            # resetting values ready for next loop
            kinesis_records = []  # empty array
            send_kinesis = False  # reset flag
            current_bytes = 0  # reset bytecount
        # regardless, make sure to incrememnt the counter for rows.
        row_count = row_count + 1
    # log out how many records were pushed
    logging.info('Total Records sent to Kinesis: {0}'.format(total_row_count))
    return "OK"


def connect_dynamo_tbl(tbl_name):
    if not dynamo:
        boto3.resource('dynamodb')
    return dynamo.Table(tbl_name)


def read_dynamo_tbl_item(tbl, key):
    try:
        response = tbl.get_item(Key=key)
    except ClientError as e:
        raise Exception(
            "Error while trying to read from {}, error message:{}".format(tbl, e.response['Error']['Message']))
    else:
        if "Item" in response:
            return 200, (response['Item'])
        else:
            return 0, "record not found in table with key {}".format(key)


def read_config_dynamo(config_tbl_name, service_name, s3path):
    config_tbl = connect_dynamo_tbl(config_tbl_name)
    return read_dynamo_tbl_item(config_tbl, {'service_pk': service_name, 's3_location_path': s3path})


def item_exist(metadata_tbl, file_name):
    (status, response) = read_dynamo_tbl_item(metadata_tbl, {'file_name': file_name})
    if status != 200:
        return False
    return True

# updates dynamo db metastore status based on lifecycle of the process
# such as reading file, converted to datafame, send to kinesis or failed status
def update_file(metadata_writer, status, file_name, file_format, dest_kds, reason="ok"):
    if status == 'reading_file':
        if item_exist(metadata_writer, file_name):
            response = metadata_writer.update_item(
                Key={
                    'file_name': file_name
                },
                UpdateExpression="set p_status=:s, updated_timestamp=:uts, reason=:r, create_timestamp=:cts",
                ExpressionAttributeValues={
                    ':s': status,
                    ':uts': datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
                    ':r': '',
                    ':cts': datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                },
                ReturnValues="UPDATED_NEW"
            )
        else:
            response = metadata_writer.put_item(
                Item={
                    'file_name': file_name,
                    'p_status': status,
                    'file_format': file_format,
                    'dest_kds': dest_kds,
                    'create_timestamp': datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                }
            )
    elif status in ['read_and_converted_to_df', 'processed_and_sent_to_kds', 'failed_at_file_read',
                    'failed_at_kinesis']:
        response = metadata_writer.update_item(
            Key={
                'file_name': file_name
            },
            UpdateExpression="set p_status=:s, updated_timestamp=:uts, reason=:r",
            ExpressionAttributeValues={
                ':s': status,
                ':uts': datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
                ':r': reason
            },
            ReturnValues="UPDATED_NEW"
        )
    return response


def get_file_separator(file_format):
    if file_format == 'comma':
        sep = ','
    elif file_format == 'tab':
        sep = '\t'
    elif file_format == 'space':
        sep = '\s+'
    elif file_format == 'pipe':
        sep = '|'
    else:
        sep = None
    return sep


def check_file_root(file_path):
    return file_path.split("://")[0]

# reads s3 file - regular, zip or gzip
def read_s3_file(file_path, is_file_zipped, sep, header):
    (bucket, file_key) = parse_s3_file_path(file_path)
    obj = s3.Object(bucket, file_key)
    if not is_file_zipped:
        body = obj.get()['Body']
        dataframe = pd.read_csv(body, sep=sep, header=header, low_memory=False)
    else:
        if ".gz" in file_key:
            body = obj.get()['Body']
            dataframe = pd.read_csv(body, sep=sep, compression='gzip', header=header, low_memory=False)
        elif ".zip" in file_key:
            s3_file = S3File(obj)
            with zipfile.ZipFile(s3_file) as zf:
                for file in zf.namelist():
                    with zf.open(file) as content:
                        dataframe = pd.read_csv(content, sep=sep, header=header, low_memory=False)
    return dataframe

# reads file with defined configuration - header_exists, file_format, file_path, is_file_zipped, column_names
# it can read file from s3 or local/NAS  path. based on parsing file_path it
# figures out whether file is in s3. The method can read comma, tab, space, pipe separated format
def read_file(file_format, header_exist, column_names, file_path, is_file_zipped):
    header = 0 if header_exist else None
    columns = [col.strip() for col in column_names.split(",")]
    sep = get_file_separator(file_format)
    try:
        if file_format in ('comma', 'tab', 'space', 'pipe'):
            if check_file_root(file_path) == "s3":
                dataframe = read_s3_file(file_path, is_file_zipped, sep, header)
            else:
                dataframe = pd.read_csv(file_path, sep=sep, header=header, low_memory=False)
        else:
            return 0, "unknown file format"
    except Exception as e:
        logging.error("Unable to parse file {} ".format(file_path))
        return 0, "Unable to parse file {}".format(file_path)
    if not header_exist:
        if len(columns) != dataframe.shape[1]:
            logging.info(
                " Number of columns[{}] in the config table is {}, but the dataset had {} number of columns".format(
                    column_names, len(columns), dataframe.shape[1]))
            return (
                0, " Number of columns[{}] in the config table is {}, but the dataset had {} number of columns".format(
                    column_names, len(columns), dataframe.shape[1]))
        else:
            dataframe.columns = columns
    return 200, dataframe


def parse_s3_file_path(s3_loc):
    s3loc_array = s3_loc.split("//")[1]
    index = s3loc_array.find("/")
    bucket = s3loc_array[0:index]
    s3_path = s3loc_array[index + 1:len(s3loc_array)]
    return bucket, s3_path

# starts the process, connects to dynamodb to read config and meta table
# reads the file from s3 and writes to kinesis
# keeps track of life cycle [reading file, writing to kinesis] in dynamodb meta table
def run_process(config_table, meta_table, service_type, file_path):
    (status, dynamo_config) = read_config_dynamo(config_table, service_type, file_path[0:file_path.rfind("/") + 1])
    if status == 0:
        raise Exception(dynamo_config)

    logging.info(dynamo_config)
    # read/write file status to dynamodb
    metadata_writer = connect_dynamo_tbl(meta_table)

    # insert loading status for file
    update_file(metadata_writer, "reading_file", file_path, dynamo_config['source_data_format'],
                dynamo_config['dest_kds_stream'])
    # read s3 file
    read_response = read_file(dynamo_config['source_data_format'], dynamo_config['header_exist'],
                              dynamo_config['column_names'], file_path, dynamo_config['is_file_zipped'])

    if read_response[0] == 200:
        # update status of file to "read_and_converted_to_df"
        update_file(metadata_writer, "read_and_converted_to_df", file_path, dynamo_config['source_data_format'],
                    dynamo_config['dest_kds_stream'])
    else:
        # update status of file to "failed_at_file_read"
        update_file(metadata_writer, "failed_at_file_read", file_path, dynamo_config['source_data_format'],
                    dynamo_config['dest_kds_stream'], reason=read_response[1])
        exit(0)
    dataframe = read_response[1]
    print(dataframe.head(3))
    # send data to kinesis
    response = send_to_kinesis(dynamo_config['dest_kds_stream'], dataframe)
    if response is not None:
        # update status of file to "processed_and_sent_to_kds"
        update_file(metadata_writer, "processed_and_sent_to_kds", file_path, dynamo_config['source_data_format'],
                    dynamo_config['dest_kds_stream'])
    else:
        # update status of file to "failed_at_kinesis"
        update_file(metadata_writer, "failed_at_kinesis", file_path, dynamo_config['source_data_format'],
                    dynamo_config['dest_kds_stream'], reason="error writing at kinesis")
    exit(1)


def main(args):
    # read config files
    file_path = args.filepath
    config_table = args.config
    meta_table = args.metastore
    service_type = args.service
    run_process(config_table, meta_table, service_type, file_path)

# This script receives the s3 file path
# and reads the file and send to kinesis
# and once received forwards it to s3_to_kinesis.py script
#   input arguments:
#     config      - name of the dynamo db config table
#     metastore   - name of the metatore table of dynamo db
#     service     - defaulted to lambda, this is the primary key in dynamodb config table
#     filepath    - absolute path of the file
# dynamodb config table defines few factors for the code to act on such as
#     data format - supported formats --> comma, tab, space, pipe
#     header_exists - a boolean , true defines head exists and false is no header
#     file_is_zipped - a boolean, true defines file is zipped, false is not zipped
#     service - defines where the script is running - lambda, EC2, ECS etc
#     file_path - defines the folder in S3 that is monitored for new file
#     column_names - name of the columns in comma separated. This is required if
#                    header_exists is false and we want to stitch column header to data
#                    at present it is required if header_exists is false

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='s3_to_kinesis_data_pumper',
                                     description='Adobe analytics sends clickstream data to S3. This application '
                                                 'reads every landed file and sends clickstream data to kinesis.')

    parser.add_argument('--config', action="store", type=str, default="carters_clickstream_config",
                        help="The name of config table in dynamo")
    parser.add_argument('--metastore', action="store", type=str, default="carters_streaming_metadata",
                        help="The name of metastore table in dynamo")
    parser.add_argument('--filepath', action="store", type=str,
                        default="s3://airflow-emr-smh/carters/clickstream/tab/test.tsv.zip",
                        help="absolute file path")
    parser.add_argument('--service', action="store", type=str,
                        default="lambda",
                        help="configuration for service")

    main(parser.parse_args())
