## POC Overview
This is a Flask-based POC for an API framework.

On MacOS with Docker installed, run this with:

	docker-compose up

Available APIs include:

```
http://localhost:5000/stats/
http://localhost:5000/tests/<count>/
http://localhost:5000/api/.../
```

A brief overview of the approach taken to count requests in redis:
1. Use a redis sorted set with the member being the API request and the score being the request count.
2. Use zincrby to create or update the request count.
3. Use the request path as the member value in the sorted set.
4. API counts are recorded within the sorted set named 'api'.

## Production Changes Required
- The test method needs to be updated to reference the production domain instead of the localhost network of the host.
- The nginx.conf needs to be updated with the production domain(s).
- Redis data should be persisted in a multiply-redundant storage environment like S3.

## Enhancements
- Add Swagger documentation.
- Add unit tests.
- Add Flask settings and use create_app().
- Provide a Postman collection.
- Hash the route to improve redis storage efficiency.
- Report exceptions to a common logging service.
- Set the Python package versions in requirements.txt.
