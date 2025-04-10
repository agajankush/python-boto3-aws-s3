import boto3
import uuid
from pydantic import BaseModel

class S3Bucket(BaseModel):
    bucket_name: str = None # Renaming the bucket and appending a random UUID
    session: object = None # The session object
    bucket_response: dict = None
    s3_resource: object = None
    s3_client: object = None
    s3_connection: object # This will be either a resource or a client
    bucket_prefic: str # Name of the s3 Bucket

    def setup_client(self):
        self.s3_resource = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
        
    def create_bucket_name(self):
        # The bucket name must be unique across all AWS S3 buckets
        return ''.join([self.bucket_prefix, str(uuid.uuid4())])

    def create_bucket(self):
        self.session = boto3.session.Session()
        current_region = self.session.region_name
        self.bucket_name = self.create_bucket_name(self.bucket_prefix)
        self.bucket_response = self.s3_connection.create_bucket(
            Bucket=self.bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': current_region
            }
        )
        print(f"Bucket {self.bucket_name} created in region {current_region}")
        return self.bucket_name, self.bucket_response

# # Using client
# first_bucket_name, first_bucket_response = create_bucket(
#     bucket_prefix= "firstpythonbucket",
#     s3_connection=s3_resource.meta.client
# )
# print(f"First bucket response: {first_bucket_response}")

# # Using resource
# second_bucket_name, second_bucket_response = create_bucket(
#     bucket_prefix= "secondpythonbucket",
#     s3_connection=s3_resource
# )
# print(f"Second bucket response: {second_bucket_response}")

# # Creating a temp file
# def create_temp_file(size, file_name, file_content):
#     random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
#     with open(random_file_name, 'w') as f:
#         f.write(str(file_content) * size)
#     return random_file_name


    