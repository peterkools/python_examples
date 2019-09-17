"""
    Using scored GDELT data, create tables reflecting potentially
    interesting statistics about scoring behavior.
"""

import boto3
import copy
import datetime
import io
import json
import pytz

AWS_PROFILE = 'sp_global'
AWS_REGION = 'us-east-1'
AWS_S3_EVENT_BUCKET = 'sp-global-events'
AWS_S3_EVENT_PATH = 'dated-events'
AWS_S3_TSV_PATH = 'tsv-events'
AWS_S3_REPORTS_PATH = 'reports'


# get_matching_s3_objects is
# copyright https://alexwlchan.net/2019/07/listing-s3-keys/
def get_matching_s3_objects(bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                return

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj


def get_matching_s3_keys(bucket, prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(bucket, prefix, suffix):
        yield obj["Key"]


def generate_counts():
    s3 = boto3.client("s3")

    # initialize stats data structure (list
    # of dictionaries where each list entry represents the hour)
    c2_values = set()
    stats = []
    template = {
        'gdelt_count': 0,
        'min_timing': 0,
        'max_timing': 0,
        'avg_timing': 0,
        'timing_sum': 0,
        'c1_true': 0,
        'c1_true_pct': 0,
        'c1_false': 0,
        'c2': {}}
    for i in range(24):
        d = copy.deepcopy(template)
        stats.append(d)

    actors = {}

    for key in get_matching_s3_keys(
            bucket=AWS_S3_EVENT_BUCKET,
            prefix='{}/'.format(AWS_S3_EVENT_PATH),
            suffix='.json'):

        # Get the S3 file
        bytes_buffer = io.BytesIO()
        s3.download_fileobj(
            Bucket=AWS_S3_EVENT_BUCKET,
            Key=key,
            Fileobj=bytes_buffer)
        byte_value = bytes_buffer.getvalue()
        str_value = byte_value.decode('utf-8')
        data = json.loads(str_value)

        # iterate over each record
        # {'event_date': '2019-08-31 12:30:1567254600',
        # 'event_hour': 12,
        # 'actor1_type1_code': 'GOV',
        # 'goldstein': '1.9',
        # 'tone': '-0.41379310344828',
        # 'latitude': '31.106',
        # 'longitude': '-97.6475',
        # 'score_class1': True,
        # 'score_class2': 3,
        # 'score_timing': 0.09399999999004649}

        for record in data:
            v = str(record['score_class2'])

            # actor stats
            actor = record['actor1_type1_code']
            if actor in actors:
                actors[actor]['gdelt_count'] += 1
                if record['score_timing'] < actors[actor]['min_timing']:
                    actors[actor]['min_timing'] = record['score_timing']
                if record['score_timing'] > actors[actor]['max_timing']:
                    actors[actor]['max_timing'] = record['score_timing']
                actors[actor]['timing_sum'] += record['score_timing']
                actors[actor]['c1_true'] += 1 if record['score_class1'] else 0
                if v in actors[actor]['c2']:
                    actors[actor]['c2'][v] += 1
                else:
                    c2_values.add(v)
                    actors[actor]['c2'][v] = 1
            else:
                actors[actor] = {}
                actors[actor]['gdelt_count'] = 1
                actors[actor]['min_timing'] = record['score_timing']
                actors[actor]['max_timing'] = record['score_timing']
                actors[actor]['timing_sum'] = record['score_timing']
                actors[actor]['c1_true'] = 1 if record['score_class1'] else 0
                actors[actor]['c2'] = {}
                actors[actor]['c2'][v] = 1

            # hourly stats
            h = record['event_hour']

            stats[h]['gdelt_count'] += 1

            if (stats[h]['min_timing'] == 0 or
                    record['score_timing'] < stats[h]['min_timing']):
                stats[h]['min_timing'] = record['score_timing']

            if record['score_timing'] > stats[h]['max_timing']:
                stats[h]['max_timing'] = record['score_timing']

            stats[h]['timing_sum'] += record['score_timing']

            if record['score_class1']:
                stats[h]['c1_true'] += 1
            else:
                stats[h]['c1_false'] += 1

            if v in stats[h]['c2']:
                stats[h]['c2'][v] += 1
            else:
                c2_values.add(v)
                stats[h]['c2'][v] = 1

    # Generate calculated actor values
    for actor in actors:
        if actors[actor]['gdelt_count']:
            actors[actor]['avg_timing'] = \
                actors[actor]['timing_sum'] / actors[actor]['gdelt_count']
            actors[actor]['c1_true_pct'] = float("%.2f" % (
                100 * actors[actor]['c1_true'] / actors[actor]['gdelt_count']))
            actors[actor]['c1_true_pct'] = '{}%'.format(actors[actor]['c1_true_pct'])
            for key in actors[actor]['c2'].keys():
                actors[actor]['c2'][key] = float("%.2f" % (
                    100 * actors[actor]['c2'][key] / actors[actor]['gdelt_count']))
                actors[actor]['c2'][key] = '{}%'.format(actors[actor]['c2'][key])

    # Generate calculated stats values
    for h in range(24):
        if stats[h]['gdelt_count']:
            stats[h]['avg_timing'] = \
                stats[h]['timing_sum'] / stats[h]['gdelt_count']
            stats[h]['c1_true_pct'] = float("%.2f" % (
                100 * stats[h]['c1_true'] / stats[h]['gdelt_count']))
            stats[h]['c1_true_pct'] = '{}%'.format(stats[h]['c1_true_pct'])
            for key in stats[h]['c2'].keys():
                stats[h]['c2'][key] = float("%.2f" % (
                    100 * stats[h]['c2'][key] / stats[h]['gdelt_count']))
                stats[h]['c2'][key] = '{}%'.format(stats[h]['c2'][key])

    # write stats to JSON file
    write_json_report(stats, actors)

    # write stats to HTML file
    write_html_report(create_stats_html(stats, actors, c2_values))


def create_stats_html(stats, actors, c2_values):
    """ Generate the HTML report page. """

    # Header
    pg = ''
    pg += """
<html>
    <head>
        <meta http-equiv="refresh" content="60">
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/materialize/1.0.0/css/materialize.min.css">
    </head>
    <body>"""

    pg += stats_html_scoring_performance(stats, c2_values)
    pg += actors_html_scoring_performance(actors, c2_values)

    # Page footer
    pg += '<p><em>This page will auto-refresh every minute. Data updates occur every 15 minutes.</em></p>'
    pg += '<p><em>Data last updated: {} UTC</em></p>'.format(datetime.datetime.strftime(datetime.datetime.now(pytz.utc), '%m/%d/%Y %H:%M:%S'))
    pg += '</body></html>'
    return pg


def stats_html_scoring_performance(stats, c2_values):
    """ Scoring performance and behavior stats by hour. """

    pg = '<h4>Scoring Performance and Behavior by Hour</h4>'
    pg += '<table cellpadding="2">'
    pg += '<tr>'
    pg += '<th>Metrics</th>'
    for h in range(24):
        pg += '<th style="white-space: nowrap; text-align: right">Hour {}</th>'.format(h)
    pg += '</tr>'

    keys = [
        ['gdelt_count', 'Record Count'],
        ['min_timing', 'Min Timing'],
        ['max_timing', 'Max Timing'],
        ['avg_timing', 'Average Timing'],
        ['c1_true_pct', 'Class1 True'],
        ['c2', 'Class2'],
    ]
    for record in keys:
        pg += '<tr>'
        key = record[0]
        label = record[1]
        # row labels
        if key != 'c2':
            pg += '<td style="white-space: nowrap">{}</td>'.format(label)

        # column values
        for h in range(24):
            if key != 'c2':
                if 'timing' in key:
                    pg += '<td style="text-align: right">{}</td>'.format(float('%.4f' % stats[h][key]))
                else:
                    pg += '<td style="text-align: right">{}</td>'.format(stats[h][key])

        if key == 'c2':
            for v in sorted(c2_values):
                pg += '<td>Class2 Value {}</td>'.format(v)
                for h in range(24):
                    if v in stats[h][key]:
                        pg += '<td style="text-align: right">{}</td>'.format(stats[h][key][v])
                    else:
                        pg += '<td style="text-align: right">0</td>'
                pg += '</tr>'
        pg += '</tr>'
    pg += '</table>'
    return pg


def actors_html_scoring_performance(actors, c2_values):
    """ Actor performance and behavior stats. """

    pg = '<h4>Actor Performance</h4>'
    pg += '<table cellpadding="2">'
    pg += '<tr>'
    pg += '<th>Actor</th>'
    pg += '<th style="white-space: nowrap; text-align: right">Record Count</th>'
    pg += '<th style="white-space: nowrap; text-align: right">Min Timing</th>'
    pg += '<th style="white-space: nowrap; text-align: right">Max Timing</th>'
    pg += '<th style="white-space: nowrap; text-align: right">Average Timing</th>'
    pg += '<th style="white-space: nowrap; text-align: right">Class1 True</th>'
    for k in sorted(c2_values):
        pg += '<th style="white-space: nowrap; text-align: right">Class2 Value {}</th>'.format(k)
    pg += '</tr>'

    for actor in sorted(actors):
        pg += '<tr>'
        record = actors[actor]
        pg += '<td style="white-space: nowrap">{}</td>'.format(actor)

        # column values
        for key in ['gdelt_count', 'min_timing', 'max_timing', 'avg_timing', 'c1_true_pct', 'c2']:
            if key != 'c2':
                if 'timing' in key:
                    pg += '<td style="text-align: right">{}</td>'.format(float('%.4f' % record[key]))
                else:
                    pg += '<td style="text-align: right">{}</td>'.format(record[key])

        if key == 'c2':
            for v in sorted(c2_values):
                if v in record[key]:
                    pg += '<td style="text-align: right">{}</td>'.format(record[key][v])
                else:
                    pg += '<td style="text-align: right">0</td>'
        pg += '</tr>'
    pg += '</table>'
    return pg


def write_json_report(stats, actors):
    """ Post the data file to the S3 bucket """
    def write_key(this_key, data):
        try:
            s3_client = boto3.client('s3')
            s3_client.put_object(
                Bucket=AWS_S3_EVENT_BUCKET,
                Body=json.dumps(data),
                Key=this_key,
                ContentType='application/json'
            )
        except Exception as exc:
            print(exc)

    this_key = '{}/stats.json'.format(AWS_S3_REPORTS_PATH)
    write_key(this_key, stats)

    this_key = '{}/actors.json'.format(AWS_S3_REPORTS_PATH)
    write_key(this_key, actors)


def write_html_report(s3_data):
    """ Post the data file to the S3 bucket """
    this_key = '{}/stats.html'.format(AWS_S3_REPORTS_PATH)

    try:
        s3_client = boto3.client('s3')
        response = s3_client.put_object(
            ACL='public-read',
            Bucket=AWS_S3_EVENT_BUCKET,
            Body=s3_data,
            Key=this_key,
            ContentType='text/html'
        )
    except Exception as exc:
        print(exc)
        return
    return False if response['ResponseMetadata']['HTTPStatusCode'] != 200 else True


def main():
    """ Processing controller """
    # Iterate over the scoring data in the S3 bucket
    generate_counts()


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
