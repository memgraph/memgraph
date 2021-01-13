# Cloud-native Graph Store

The biggest problem with the current in-memory storage is the total cost of
ownership for large datasets non-frequently updated. An idea to solve that is a
decoupled storage and compute inside a cloud environment. E.g., on AWS, a
database instance could use EC2 machines to run the query execution against
data stored inside S3.
