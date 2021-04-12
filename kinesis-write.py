# necessary imports
import time

import boto3
import pandas as pd


# function for sending data to Kinesis at the absolute maximum throughput
def send_to_kinesis(kinesis_client, kinesis_stream_name, kinesis_shard_count, data):
    kinesisRecords = []  # empty list to store data

    (rows, columns) = data.shape  # get rows and columns off provided data

    currentBytes = 0  # counter for bytes

    rowCount = 0  # as we start with the first

    totalRowCount = rows  # using our rows variable we got earlier

    sendKinesis = False  # flag to update when it's time to send data

    shardCount = 1  # shard counter

    # loop over each of the data rows received
    for _, row in data.iterrows():

        values = '|'.join(str(value) for value in row)  # join the values together by a '|'

        encodedValues = bytes(values, 'utf-8')  # encode the string to bytes

        # create a dict object of the row
        kinesisRecord = {
            "Data": encodedValues,  # data byte-encoded
            "PartitionKey": str(shardCount)  # some key used to tell Kinesis which shard to use
        }

        kinesisRecords.append(kinesisRecord)  # add the object to the list
        stringBytes = len(values.encode('utf-8'))  # get the number of bytes from the string
        currentBytes = currentBytes + stringBytes  # keep a running total

        # check conditional whether ready to send
        if len(kinesisRecords) == 500:  # if we have 500 records packed up, then proceed
            sendKinesis = True  # set the flag

        if currentBytes > 50000:  # if the byte size is over 50000, proceed
            sendKinesis = True  # set the flag

        if rowCount == totalRowCount - 1:  # if we've reached the last record in the results
            sendKinesis = True  # set the flag

        # if the flag is set
        if sendKinesis == True:

            # put the records to kinesis
            response = kinesis_client.put_records(
                Records=kinesisRecords,
                StreamName=kinesis_stream_name
            )

            # resetting values ready for next loop
            kinesisRecords = []  # empty array
            sendKinesis = False  # reset flag
            currentBytes = 0  # reset bytecount

            # increment shard count after each put
            shardCount = shardCount + 1

            # if it's hit the max, reset
            if shardCount > kinesis_shard_count:
                shardCount = 1

        # regardless, make sure to incrememnt the counter for rows.
        rowCount = rowCount + 1

    # log out how many records were pushed
    print('Total Records sent to Kinesis: {0}'.format(totalRowCount))

def run():
    # start timer
    start = time.time()

    # create a client with kinesis
    kinesis = boto3.client('kinesis', region_name='us-west-2')

    # load in data
    data = pd.read_csv('data.txt', sep=" ", header=None)

    # send it to kinesis data stream
    stream_name = "wordcount-stream"
    stream_shard_count = 1
    while True:
        send_to_kinesis(kinesis, stream_name, stream_shard_count, data)  # send it!

    # end timer
    end = time.time()

    # log time
    print("Runtime: " + str(end - start))


if __name__ == "__main__":
    # run main
    run()