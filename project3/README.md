### Project3

This project acquires the newest data from GDELT (a news categorization service), scores it using a public
scoring API from S&P Global and emits statistics about the scoring results. This approach uses AWS Lambda,
SQS and RDS to implement a scalable data generation example. It does not currently include a presentation
view of the scored data.

This was designed as a proof of concept and is not a production example.

#### Operations Notes

This project requires a VPC-based AWS configuration with a NAT for public Internet access.
It utilizes RDS MySQL, Lambda, CloudWatch events (to trigger one Lambda) and SQS. Secrets should
be moved into AWS secret storage.

