import os
import datetime
import boto3
from botocore.exceptions import ClientError
from botocore.client import BaseClient
import threading
from typing import Optional, List
import configparser

class S3ClientWrapper:
    BUFFER_PERIOD_MINUTES: int = 10

    def __init__(self, endpoint_url: str, region: Optional[str] = None) -> None:
        self.endpoint_url: str = endpoint_url
        self.region: Optional[str] = region
        self._lock: threading.Lock = threading.Lock()
        self._s3_client: Optional[BaseClient] = None
        self._expiry_time: Optional[datetime.datetime] = None
        self._refresh_s3_client(force_refresh=True)

    def _read_aws_credentials_expiry(self) -> Optional[datetime.datetime]:
        aws_credentials_path = os.path.expanduser('~/.aws/credentials')
        config = configparser.ConfigParser()
        try:
            config.read(aws_credentials_path)
            expiry_str = config.get('default', 'expiration', fallback=None)
            if expiry_str:
                return datetime.datetime.fromisoformat(expiry_str).astimezone(datetime.timezone.utc)
        except Exception as e:
            print(f"Error reading expiration from AWS credentials: {e}")
        return None

    def _create_s3_client(self) -> Optional[BaseClient]:
        try:
            session = boto3.session.Session()
            client = session.client('s3', endpoint_url=self.endpoint_url, region_name=self.region)
            self._expiry_time = self._read_aws_credentials_expiry()  # Update expiry time upon client creation
            print(f"New client created, expiration time refreshed: {self._expiry_time}")
            return client
        except Exception as e:
            print(f"Failed to create S3 client: {e}")
            raise e

    def _refresh_s3_client(self, force_refresh: bool = False) -> None:
        with self._lock:
            current_time = datetime.datetime.now(datetime.timezone.utc)
            if force_refresh or not self._expiry_time or (self._expiry_time and current_time >= (self._expiry_time - datetime.timedelta(minutes=self.BUFFER_PERIOD_MINUTES))):
                print("Refreshing S3 client due to credential expiry or forced refresh.")
                self._s3_client = self._create_s3_client()
            else:
                print(f"No need Refreshing S3 client self._expiry_time={self._expiry_time}")

    def get_fresh_s3_client(self) -> Optional[BaseClient]:
        self._refresh_s3_client()
        return self._s3_client