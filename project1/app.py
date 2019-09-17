'''
    Overview of the approach
    1. Use a redis sorted set with the member being the API request
       and the score being the request count
    2. Use zincrby to create or update the request count
    3. Use the API request as the member value in the sorted set
    4. API counts are recorded within the sorted set named 'api'
'''

from flask import Flask, jsonify, request
from http import HTTPStatus
import random
from redis import Redis, RedisError
import requests

# Connect to Redis
redis = Redis(
    host="redis",
    db=0,
    socket_connect_timeout=2,
    socket_timeout=2,
    decode_responses=True)

REDIS_LOG_KEY_NAME = 'api'
REDIS_INT64_MAX = 9223372036854775807  # (2 power 63 -1)

app = Flask(__name__)


# The specification is inconsistent with regard to the number of allowed
# path segments so I've chosen to support the maximum discussed by in
# spec #4 rather than the arbitrary number discussed in spec #2.
@app.route("/api/<p1>/<p2>/<p3>/<p4>/<p5>/<p6>/", methods=['GET'])
@app.route("/api/<p1>/<p2>/<p3>/<p4>/<p5>/", methods=['GET'])
@app.route("/api/<p1>/<p2>/<p3>/<p4>/", methods=['GET'])
@app.route("/api/<p1>/<p2>/<p3>/", methods=['GET'])
@app.route("/api/<p1>/<p2>/", methods=['GET'])
@app.route("/api/<p1>/", methods=['GET'])
def api_record(p1, p2=None, p3=None, p4=None, p5=None, p6=None):
    """
        Tally the API request in redis.

        Accepts paths up to six deep. For example:
            http://localhost:5000/api/<p1>/<p2>/<p3>/<p4>/<p5>/<p6>/
    """
    exception = log_api()
    if exception:
        return jsonify({'error': exception}), HTTPStatus.INTERNAL_SERVER_ERROR
    return '', HTTPStatus.OK


@app.route("/stats/", methods=['GET'])
def stats():
    """
        Reports API calls in highest to lowest usage order.
    """
    # Log all API requests
    exception = log_api()
    if exception:
        return jsonify({'error': exception}), HTTPStatus.INTERNAL_SERVER_ERROR

    try:
        data = redis.zrevrangebyscore(
            REDIS_LOG_KEY_NAME,
            REDIS_INT64_MAX,
            0,
            withscores=True)

        # The redis response is an ordered list of lists:
        #   [["/stats",13.0],["/api/1/2/3/4/5",6.0]]
        # and I prefer an ordered list of dictionaries so the
        # caller doesn't need to guess which is which.
        # Ordering is by descending request count
        response_data = []
        for row in data:
            response_data.append({'count': row[1], 'url': row[0]})
        return jsonify(response_data), HTTPStatus.OK
    except RedisError as exc:
        return jsonify({'error': exc}), HTTPStatus.INTERNAL_SERVER_ERROR


@app.route("/test/<int:count>/", methods=["POST"])
def api_test(count):
    """
        Generates <count> test URLs and submits them.
    """
    # Log all API requests
    exception = log_api()
    if exception:
        return jsonify({'error': exception}), HTTPStatus.INTERNAL_SERVER_ERROR

    # Per the spec, path segments are used across all requests within a test
    path_segments = generate_path_segments()
    for i in range(0, count):
        # Randomly determine the number of segments in this request
        path_count = random.randrange(1, 7)

        # WARNING
        # host.docker.internal is NOT production safe.
        # The production domain should be taken from settings
        # or the environment.
        url = 'http://host.docker.internal:5000/api'

        while path_count > 0:
            url += '/{}'.format(path_segments[random.randrange(0, 3)])
            path_count -= 1
        url += '/'

        try:
            requests.get(url)
        except requests.exceptions.RequestException:
            return jsonify({'error': 'request error'}), \
                HTTPStatus.INTERNAL_SERVER_ERROR
    return '', HTTPStatus.OK


def generate_path_segments():
    """
        Generate and return a list of three random strings.
    """

    # Constrain path characters to alphanumeric and
    # skip lookalikes (1, i, l, 0, o)
    allowed = \
        'abcdefghjkmnpqrstuvwxyz' \
        'ABCDEFGHJKMNPQRSTUVWXYZ' \
        '23456789'

    segs = []
    for index in range(3):
        # I will allow API path segments to be 3-8 characters
        str_len = random.randrange(3, 9)
        segs.append(''.join(random.choice(allowed) for i in range(str_len)))
    return segs


def log_api():
    """
        Log the API request in redis.
    """
    try:
        redis.zincrby(REDIS_LOG_KEY_NAME, 1, request.path)
    except RedisError as exc:
        return exc


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=False)
