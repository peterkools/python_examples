"""
    Score and record GDELT events.

    To install into Lambda, we need to create a zip package
    cp gdelt_score_events.py ../lib/python3.7/site-packages/lambda_function.py
    pushd ../lib/python3.7/site-packages
    zip -r XXX.zip * 

   The zip file list can be restricted to just the packages to reduce the size.
"""

import base64
import boto3
import datetime
import json
import pymysql
import requests

AWS_PROFILE = 'sp_global'
AWS_REGION = 'us-east-1'

# RDS credentials should be in a secret instead of here.
RDS_HOST = 'pwk1.carmnypphv56.us-east-1.rds.amazonaws.com'
RDS_USER = 'dbroot'
RDS_PASS = 'PYB42tPWRBNc4Afy'
RDS_DB = 'gdelt'

# RDS_HOST = 'localhost'
# RDS_USER = 'XXXX'
# RDS_PASS = 'XXXX'
# RDS_DB = 'gdelt'

AWS_SQS_GDELT_SCORE = 'gdelt_score'

# API credentials should be in a secret instead of here
SPG_MODEL_URL = 'https://app-models.dominodatalab.com:443/models/5bd0856346e0fb0008d06d74/latest/model'  # noqa: E501
SPG_AUTH = '58BTaPnrmzIDtI0VrVVz6v6qKOu8ABmYZzDGhTmaoW7xgddOhx9ISGdAndVVdziE'


def format_date(d):
    """ GDELT dates are YYYYMMDDHHMMSS """
    dt = datetime.datetime.strptime(d, '%Y%m%d%H%M%S')
    return datetime.datetime.strftime(dt, '%Y-%m-%d %H:%M:%S')


def score(scoring_record):
    """ Score the record. """

    user_pass = base64.b64encode('{}:{}'.format(
        SPG_AUTH,
        SPG_AUTH).encode('utf-8')).decode('utf-8')
    data = json.dumps({'data': scoring_record})
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic {}'.format(user_pass)}

    try:
        response = requests.post(SPG_MODEL_URL, data=data, headers=headers)
    except Exception as exc:
        print(exc)
        return

    spg_score = response.json()
    return """('{0}', {1}, {2}, {3}, {4}, '{5}', {6}, {7}, {8})""".format(
        scoring_record['actor_code'],
        scoring_record['goldstein'],
        scoring_record['avg_tone'],
        scoring_record['lat'] if scoring_record['lat'] else 0,
        scoring_record['lon'] if scoring_record['lon'] else 0,
        format_date(scoring_record['date']),
        1 if spg_score['result']['class1'] else 0,
        spg_score['result']['class2'],
        spg_score['timing'])


def insert(values):
    """ Bulk insert the scored records. """

    try:
        conn = pymysql.connect(
            RDS_HOST,
            user=RDS_USER,
            passwd=RDS_PASS,
            db=RDS_DB,
            connect_timeout=5,
            autocommit=1)
    except pymysql.MySQLError as e:
        print('MySQL connection failure: {}'.format(e))
        return

    # auto-commit
    conn.commit()

    cursor = conn.cursor()
    sql = """
INSERT INTO api1
(actor1_code, goldstein, avg_tone, lat, lon, event_date,
class1, class2, timing)
VALUES """
    sql = sql + ', '.join(values)

    try:
        cursor.execute(sql)
    except pymysql.MySQLError as e:
        print('MySQL insert error: {}'.format(e))
        print(sql)

    conn.close()


def main():
    """ Processing controller """
    sqs = boto3.resource('sqs')
    sub_queue = sqs.get_queue_by_name(QueueName=AWS_SQS_GDELT_SCORE)

    messages = sub_queue.receive_messages(
        AttributeNames=['All'],
        MaxNumberOfMessages=10)

    values = []
    for message in messages:
        value = score(json.loads(message.body))
        if value:
            values.append(value)
        message.delete()

    insert(values)


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
