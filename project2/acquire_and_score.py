"""
    Acquire, score and record GDELT quarter hour updates.
"""

import base64
import boto3
import datetime
from io import BytesIO
import json
import os
import pytz
from urllib.request import urlopen
from zipfile import ZipFile
import requests

AWS_PROFILE = 'sp_global'
AWS_REGION = 'us-east-1'
AWS_S3_EVENT_BUCKET = 'sp-global-events'
AWS_S3_EVENT_PATH = 'dated-events'
AWS_S3_TSV_PATH = 'tsv-events'

SPG_MODEL_URL = 'https://app-models.dominodatalab.com:443/models/5bd0856346e0fb0008d06d74/latest/model'
SPG_AUTH = 'XXXX'

GDELT_LATEST = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'

GDELT_EVENT_FILE_NAME_PART = 'export.CSV.zip'
GDELT_EVENT_INDEX_GLOBALEVENTID = 0
GDELT_EVENT_INDEX_Actor1Type1Code = 12
GDELT_EVENT_INDEX_GoldsteinScale = 30
GDELT_EVENT_INDEX_AvgTone = 34
GDELT_EVENT_INDEX_Actor1Geo_Lat = 40
GDELT_EVENT_INDEX_Actor1Geo_Long = 41
GDELT_EVENT_INDEX_DATEADDED = 59

GDELT_RECORD_LABLES = [
    'GLOBALEVENTID',
    'SQLDATE',
    'MonthYear',
    'Year',
    'FractionDate',
    'Actor1Code',
    'Actor1Name',
    'Actor1CountryCode',
    'Actor1KnownGroupCode',
    'Actor1EthnicCode',
    'Actor1Religion1Code',
    'Actor1Religion2Code',
    'Actor1Type1Code',
    'Actor1Type2Code',
    'Actor1Type3Code',
    'Actor2Code',
    'Actor2Name',
    'Actor2CountryCode',
    'Actor2KnownGroupCode',
    'Actor2EthnicCode',
    'Actor2Religion1Code',
    'Actor2Religion2Code',
    'Actor2Type1Code',
    'Actor2Type2Code',
    'Actor2Type3Code',
    'IsRootEvent',
    'EventCode',
    'EventBaseCode',
    'EventRootCode',
    'QuadClass',
    'GoldsteinScale',
    'NumMentions',
    'NumSources',
    'NumArticles',
    'AvgTone',
    'Actor1Geo_Type',
    'Actor1Geo_FullName',
    'Actor1Geo_CountryCode',
    'Actor1Geo_ADM1Code',
    'Actor1Geo_ADM2Code',
    'Actor1Geo_Lat',
    'Actor1Geo_Long',
    'Actor1Geo_FeatureID',
    'Actor2Geo_Type',
    'Actor2Geo_FullName',
    'Actor2Geo_CountryCode',
    'Actor2Geo_ADM1Code',
    'Actor2Geo_ADM2Code',
    'Actor2Geo_Lat',
    'Actor2Geo_Long',
    'Actor2Geo_FeatureID',
    'ActionGeo_Type',
    'ActionGeo_FullName',
    'ActionGeo_CountryCode',
    'ActionGeo_ADM1Code',
    'ActionGeo_ADM2Code',
    'ActionGeo_Lat',
    'ActionGeo_Long',
    'ActionGeo_FeatureID',
    'DATEADDED',
    'SOURCEURL'
]


def get_latest_event_file_url():
    """
        Get the most recent event file.
    """

    try:
        response = urlopen(GDELT_LATEST)
    except Exception:
        print('Exception getting {}'.format(GDELT_LATEST))
        return

    for line in response.readlines():
        line = line.decode('utf-8').strip('\n')
        parts = line.split(' ')
        file_url = parts[-1]
        if GDELT_EVENT_FILE_NAME_PART in file_url:
            return file_url


def format_date(d):
    """
        GDELT dates are YYYYMMDDHHMMSS.
        The model API requires YYYY-MM-DD HH:MM:SS.
    """
    dt = datetime.datetime.strptime(d, '%Y%m%d%H%M%S')
    return datetime.datetime.strftime(dt, '%Y-%m-%d %H:%M:%s'), dt.hour


def generate_gdelt_record(parts):
    """ Emit a JSON reflection of the labled GDELT record """
    d = {}
    for index in range(len(GDELT_RECORD_LABLES)):
        d[GDELT_RECORD_LABLES[index]] = parts[index]
    return d


def score(gdelt_record, scoring_record):
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
    return spg_score['result']['class1'], spg_score['result']['class2'], spg_score['timing']


def write_s3_file(filename, s3_data):
    """ Post the data file to the S3 bucket """
    this_key = '{}/{}.json'.format(AWS_S3_EVENT_PATH, filename)

    try:
        s3_client = boto3.client('s3')
        response = s3_client.put_object(
            Bucket=AWS_S3_EVENT_BUCKET,
            Body=json.dumps(s3_data),
            Key=this_key,
            ContentType='application/json'
        )
    except Exception as exc:
        print(exc)
        return

    return False if response['ResponseMetadata']['HTTPStatusCode'] != 200 else True


def process_event_file(file_url):
    """
        Get the event file.
        Each line with a Actor1Type1Code gets scored.
        One file is written to S3 with the record set.
    """

    count = 0
    try:
        response = urlopen(file_url)
    except Exception as exc:
        print(exc)
        return

    zipfile = ZipFile(BytesIO(response.read()))
    filename = os.path.basename(file_url[:-4])
    data = []
    for line in zipfile.open(filename).readlines():
        parts = line.decode('utf-8').strip('\n').split('\t')

        # Some event records to not have an Actor1Type1Code
        # we will skip these
        if not parts[GDELT_EVENT_INDEX_Actor1Type1Code]:
            continue

        gdelt_record = generate_gdelt_record(parts)
        gdate, ghour = format_date(parts[GDELT_EVENT_INDEX_DATEADDED])

        scoring_parts = {
            'actor_code': parts[GDELT_EVENT_INDEX_Actor1Type1Code],
            'goldstein': parts[GDELT_EVENT_INDEX_GoldsteinScale],
            'avg_tone': parts[GDELT_EVENT_INDEX_AvgTone],
            'lat': parts[GDELT_EVENT_INDEX_Actor1Geo_Lat],
            'lon': parts[GDELT_EVENT_INDEX_Actor1Geo_Long],
            'date': gdate,
        }
        class1, class2, timing = score(gdelt_record, scoring_parts)

        record = {
            'event_date': gdate,
            'event_hour': ghour,
            'actor1_type1_code': scoring_parts['actor_code'],
            'goldstein': scoring_parts['goldstein'],
            'tone': scoring_parts['avg_tone'],
            'latitude': scoring_parts['lat'],
            'longitude': scoring_parts['lon'],
            'score_class1': class1,
            'score_class2': class2,
            'score_timing': timing,
        }
        data.append(record)
        count += 1

    s3_filename = filename[:-11]
    write_s3_file(s3_filename, data)
    print('{}: created {} S3 records'.format(
       datetime.datetime.now(pytz.utc),
       count))


def main():
    """ Processing controller """
    file_url = get_latest_event_file_url()
    if file_url:
        print(file_url)
        process_event_file(file_url)


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
