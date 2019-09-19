"""
    Acquire and parse the GDELT file, publishing each
    event on the GDELT scoring queue.
"""

import boto3
from io import BytesIO
import json
import os
from urllib.request import urlopen
from zipfile import ZipFile

AWS_PROFILE = 'sp_global'
AWS_REGION = 'us-east-1'

AWS_SQS_GDELT_LATEST = 'gdelt_latest'
AWS_SQS_GDELT_SCORE = 'gdelt_score'

GDELT_EVENT_INDEX_GLOBALEVENTID = 0
GDELT_EVENT_INDEX_Actor1Type1Code = 12
GDELT_EVENT_INDEX_GoldsteinScale = 30
GDELT_EVENT_INDEX_AvgTone = 34
GDELT_EVENT_INDEX_Actor1Geo_Lat = 40
GDELT_EVENT_INDEX_Actor1Geo_Long = 41
GDELT_EVENT_INDEX_DATEADDED = 59


def process_event_file(pub_queue, file_url):
    """
        Get the event file.
        Each line with an Actor1Type1Code gets published.
    """

    if not file_url:
        print('No file URL in message body')
        return

    try:
        response = urlopen(file_url)
    except Exception as exc:
        print('Exception getting the GDELT file: {}'.format(exc))
        return

    zipfile = ZipFile(BytesIO(response.read()))
    file_url = os.path.basename(file_url[:-4])
    count = 1

    for line in zipfile.open(file_url).readlines():
        parts = line.decode('utf-8').strip('\n').split('\t')

        # Skip event records that don't have an Actor1Type1Code
        if not parts[GDELT_EVENT_INDEX_Actor1Type1Code]:
            continue

        scoring_parts = {
            'actor_code': parts[GDELT_EVENT_INDEX_Actor1Type1Code],
            'goldstein': parts[GDELT_EVENT_INDEX_GoldsteinScale],
            'avg_tone': parts[GDELT_EVENT_INDEX_AvgTone],
            'lat': parts[GDELT_EVENT_INDEX_Actor1Geo_Lat],
            'lon': parts[GDELT_EVENT_INDEX_Actor1Geo_Long],
            'date': parts[GDELT_EVENT_INDEX_DATEADDED],
        }

        # Batch messages for API efficiency and speed of processing
        if count == 1:
            messages = []

        messages.append(prepare(count, scoring_parts))

        if count == 10:
            publish(pub_queue, messages)
            count = 1
            messages = []
        count += 1

    # Publish remaining events after parsing the GDELT file
    messages.append(prepare(count, scoring_parts))
    publish(pub_queue, messages)


def prepare(count, scoring_parts):
    """ Allocate new dictionary for the data, format and return """
    data = {'Id': str(count), 'MessageBody': json.dumps(scoring_parts)}
    return data


def publish(pub_queue, messages):
    """ Publish the message set """
    try:
        pub_queue.send_messages(Entries=messages)
    except Exception as exc:
        print('Exception sending message: {}'.format(exc))


def main():
    """ Processing controller """
    sqs = boto3.resource('sqs')
    sub_queue = sqs.get_queue_by_name(QueueName=AWS_SQS_GDELT_LATEST)
    pub_queue = sqs.get_queue_by_name(QueueName=AWS_SQS_GDELT_SCORE)

    messages = sub_queue.receive_messages(
        AttributeNames=['All'],
        MaxNumberOfMessages=1)
    for message in messages:
        process_event_file(pub_queue, message.body)
        message.delete()


def lambda_handler(event, context):
    # Within Lambda, boto initialization uses the IAM role available
    boto3.setup_default_session(region_name=AWS_REGION)
    main()


if __name__ == "__main__":
    # Outside Lambda, boto uses AWS credentials
    boto3.setup_default_session(
        region_name=AWS_REGION,
        profile_name=AWS_PROFILE)
    main()
