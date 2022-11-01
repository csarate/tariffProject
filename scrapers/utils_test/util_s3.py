import boto3
import botocore

event: dict = {}


def set_event(exec_event: dict):
    global event
    event = exec_event


# ################################## #
# Check if S3 object exits in bucket #
# ################################## #
def file_exists(str_s3_file: str) -> bool:
    s3 = boto3.resource('s3')

    try:
        s3.Object(event["S3Bucket"], str_s3_file).load()
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does exist.
            return False
        else:
            # Something else has gone wrong.
            raise


# ############################ #
# Load S3 Object from a bucket #
# ############################ #
def load_file(str_s3_file: str) -> str:
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(event["S3Bucket"])
    s3_file = bucket.Object(str_s3_file)
    text: str = s3_file.get()['Body'].read().decode('UTF8')

    return text


# ############################ #
# Save S3 Object into a bucket #
# ############################ #
def save_file(str_s3_file: str, str_s3_content: str):
    s3 = boto3.resource('s3')
    s3_file = s3.Object(event["S3Bucket"], str_s3_file)
    s3_file.put(Body=str_s3_content)


# ############################## #
# Delete S3 Object from a bucket #
# ############################## #
def delete_file(str_s3_file: str):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(event["S3Bucket"])
    s3_file = bucket.Object(str_s3_file)
    s3_file.delete()


# ################################ #
# Return list of files in a folder #
# ################################ #
def files_in_folder(str_s3_folder: str, str_s3_file_extension: str = "") -> str:
    lst_files: list = []
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(event["S3Bucket"])
    for s3_object in bucket.objects.filter(Prefix=str_s3_folder):
        if s3_object.key.endswith(str_s3_file_extension):
            lst_files.append(s3_object.key)

    return lst_files
