import boto3
import io
import os

from urllib.parse import unquote_plus


class S3File(io.RawIOBase):
    def __init__(self, s3_object):
        self.s3_object = s3_object
        self.position = 0

    def __repr__(self):
        return "<%s s3_object=%r>" % (type(self).__name__, self.s3_object)

    @property
    def size(self):
        return self.s3_object.content_length

    def tell(self):
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = self.size + offset
        else:
            raise ValueError(
                "invalid whence (%r, should be %d, %d, %d)"
                % (whence, io.SEEK_SET, io.SEEK_CUR, io.SEEK_END)
            )

        return self.position

    def seekable(self):
        return True

    def read(self, size=-1, lines=-1):
        if size == -1:
            range_header = "bytes=%d-" % self.position
            self.seek(offset=0, whence=io.SEEK_END)
        else:
            new_position = self.position + size

            if new_position >= self.size:
                return self.read()

            range_header = "bytes=%d-%d" % (self.position, new_position - 1)
            self.seek(offset=size, whence=io.SEEK_CUR)

        return self.s3_object.get(Range=range_header)["Body"].read()

    def readable(self):
        return True


def lambda_handler(event, context):
    s3r = boto3.resource("s3")
    s3c = boto3.client("s3")
    chunk_size = 5 * 1024 * 1024 * 10

    for record in event["Records"]:

        bucket = record["s3"]["bucket"]["name"]
        source_key = unquote_plus(record["s3"]["object"]["key"])
        target_key = "/".join(["utf-8"] + source_key.split("/")[1:])
        print(f"Processing file {source_key}")

        create_mpu_response = s3c.create_multipart_upload(
            Bucket=bucket, Key=target_key,
        )
        upload_id = create_mpu_response["UploadId"]
        print("Upload id: %s" % upload_id)

        s3_object = s3r.Object(bucket_name=bucket, key=source_key)

        s3_file = S3File(s3_object)

        total_parts, leftover = divmod(s3_object.content_length, chunk_size)
        print(f"Total parts: {total_parts}")

        file_parts = {"Parts": []}

        for n in range(1, total_parts + 2):
            print(n)

            chunk_to_read = leftover if n == total_parts + 2 else chunk_size
            data = s3_file.read(chunk_to_read).decode(
                encoding=os.environ("SOURCE_ENCODING")
            )

            upload_part_response = s3c.upload_part(
                Bucket=bucket,
                Key=target_key,
                UploadId=upload_id,
                PartNumber=n,
                Body=data.encode("utf-8"),
            )

            file_parts["Parts"].append(
                {"ETag": upload_part_response["ETag"], "PartNumber": n}
            )

        _ = s3c.complete_multipart_upload(
            Bucket=bucket,
            Key=target_key,
            UploadId=upload_id,
            MultipartUpload=file_parts,
        )


if __name__ == "__main__":
    records = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": os.environ("BUCKET")},
                    "object": {"key": os.environ("S3_FILE")},
                }
            }
        ]
    }
    lambda_handler(records, None)
