"""
    Acquire the latest GDELT file name and publish it to the queue.

    The Lambda function is triggered by a CloudWatch rule
    (gdelt_trigger_latest_file) that runs every 15 minutes.

"""

import boto3
from urllib.request import urlopen

AWS_PROFILE = 'sp_global'
AWS_REGION = 'us-east-1'

AWS_SQS_GDELT_LATEST = 'gdelt_latest'

GDELT_LATEST_FILE_NAME = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
GDELT_EVENT_FILE_NAME_PART = 'export.CSV.zip'


def get_latest_event_filename():
    """ Return the name of the most recent GDELT event file """

    try:
        response = urlopen(GDELT_LATEST_FILE_NAME)
    except Exception:
        print('Exception getting {}'.format(GDELT_LATEST_FILE_NAME))
        return

    for line in response.readlines():
        filename = line.decode('utf-8').strip('\n').split(' ')[-1]
        if GDELT_EVENT_FILE_NAME_PART in filename:
            return filename


def publish_filename(filename=None):
    """ Publish the GDELT file name onto the queue """
    if not filename:
        print('No GDELT url to publish')
        return

    sqs = boto3.resource('sqs')
    try:
        queue = sqs.get_queue_by_name(QueueName=AWS_SQS_GDELT_LATEST)
    except Exception as exc:
        print('Exception getting queue URL: {}'.format(exc))
        return

    try:
        queue.send_message(MessageBody=filename)
    except Exception as exc:
        print('Exception sending message: {}'.format(exc))


def main():
    """ Processing controller """
    publish_filename(get_latest_event_filename())


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
