import os


class Config:
    """
    Configuration file.
    """

    def __init__(self):
        """
        Loads the configuration file using environment.
        """
        self.s3_bucket = os.getenv('BIOINDEX_S3_BUCKET')
        self.rds_instance = os.getenv('BIOINDEX_RDS_INSTANCE')

        # optional settings with reasonable defaults
        server_response_limit = int(os.getenv('BIOINDEX_RESPONSE_LIMIT', 1 * 1024 * 1024))

        # validate settings
        assert self.s3_bucket
        assert self.rds_instance
