import os
import logging
import concurrent.futures
import time
from typing import List, Optional
from database import Database
from s3_client_wrapper import S3ClientWrapper
from botocore.exceptions import ClientError

class S3MeltdownError(Exception):
    """Raised when we detect an S3 meltdown/offline situation, to skip further uploads."""
    pass

class S3ImageUploader:
    def __init__(
        self,
        db_config: dict,
        s3_config: dict,
        max_workers: int = 3,
        meltdown_threshold: int = 5,
        sleep_time: int = 30
    ):
        """
        :param db_config: Database configuration dict with user, password, host, port, database.
        :param s3_config: S3 configuration dict with endpoint_url, optional region, etc.
        :param max_workers: Max number of threads for concurrent uploads.
        :param meltdown_threshold: Number of meltdown-level errors tolerated before stopping.
        :param sleep_time: Number of seconds to sleep when no pending images are found.
        """
        self.db = Database.get_instance()
        self.db.initialize_connection_pool(
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database']
        )

        self.s3_client_wrapper = S3ClientWrapper(
            endpoint_url=s3_config['endpoint_url'],
            region=s3_config.get('region')
        )
        self.max_workers = max_workers
        self.meltdown_threshold = meltdown_threshold
        self.consecutive_meltdown_errors = 0
        self.sleep_time = sleep_time  # <--- Sleep time now stored here

    def upload_image(self, image_record: dict):
        """
        Attempts to upload a single image to S3.
        If successful, inserts into `uploaded_s3` and deletes the row from `upload_to_s3`.
        If failure, marks the row as 'failed'.
        Potential meltdown is detected if repeated service-level errors happen.
        """
        upload_id = image_record['id']
        local_path = image_record['local_path']
        image_id = image_record['image_id']
        acq_id = image_record['acq_id']
        s3_path = local_path.lstrip('/')  # Remove any leading slashes
        bucket_name = 'mikro'

        if not os.path.isfile(local_path):
            error_msg = f"Local file {local_path} does not exist."
            logging.error(error_msg)
            self.db.mark_as_failed(upload_id, error_msg)
            return

        try:
            s3_client = self.s3_client_wrapper.get_fresh_s3_client()

            # Check if file exists in S3
            if self.file_exists_in_s3(s3_client, bucket_name, s3_path):
                logging.info(f"S3 already has {s3_path}. Removing DB row for upload_id={upload_id}.")
                self.db.delete_uploaded_record(upload_id)
                return

            # Attempt upload
            self.upload_file_to_s3(s3_client, bucket_name, local_path, s3_path)

            # Insert into 'uploaded_s3' table
            self.db.insert_into_uploaded_s3(
                image_id=image_id,
                acq_id=acq_id,
                local_path=local_path,
                s3_path=s3_path,
                bucket_name=bucket_name
            )

            # If successful, delete record
            self.db.delete_uploaded_record(upload_id)
            logging.info(f"Successfully uploaded ID {upload_id} → s3://{bucket_name}/{s3_path} and removed from DB.")

            # Reset meltdown error count on success
            self.consecutive_meltdown_errors = 0

        except S3MeltdownError:
            # Bubble up meltdown to stop further processing
            raise
        except Exception as e:
            # Normal failure
            error_msg = f"Failed to upload image ID {upload_id}: {str(e)}"
            logging.exception(error_msg)
            self.db.mark_as_failed(upload_id, str(e))

    def file_exists_in_s3(self, s3_client, bucket_name: str, s3_path: str) -> bool:
        """
        Checks if the specified object already exists in the S3 bucket.
        Returns True if it exists, False otherwise.
        """
        from botocore.exceptions import ClientError
        try:
            s3_client.head_object(Bucket=bucket_name, Key=s3_path)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            # If it's a 404, object not found -> return False
            if error_code == '404':
                return False

            # Potential meltdown check: e.g. 503 or connection-level issue
            if error_code in ['503', 'RequestTimeout', 'ServiceUnavailable']:
                self.consecutive_meltdown_errors += 1
                if self.consecutive_meltdown_errors >= self.meltdown_threshold:
                    logging.error("S3 meltdown detected (consecutive service errors).")
                    raise S3MeltdownError("Too many consecutive meltdown-level errors.")
            # Re-raise for normal handling
            raise e

    def upload_file_to_s3(self, s3_client, bucket_name: str, local_path: str, s3_path: str):
        """
        Decides which upload approach to use (multi-part vs. single-part).
        For simplicity, we use put_object for single-part in this example.
        """
        self.upload_file_to_s3_non_multipart(s3_client, bucket_name, local_path, s3_path)

    def upload_file_to_s3_non_multipart(self, s3_client, bucket_name: str, local_path: str, s3_path: str):
        """
        Uploads a file to an S3 bucket using put_object. Single-part approach.
        """
        from botocore.exceptions import ClientError
        try:
            with open(local_path, 'rb') as file:
                s3_client.put_object(Bucket=bucket_name, Key=s3_path, Body=file)
                logging.info(f"Uploaded {local_path} → s3://{bucket_name}/{s3_path}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['503', 'RequestTimeout', 'ServiceUnavailable']:
                self.consecutive_meltdown_errors += 1
                if self.consecutive_meltdown_errors >= self.meltdown_threshold:
                    logging.error("S3 meltdown detected (upload).")
                    raise S3MeltdownError("Too many consecutive meltdown-level errors.")
            # Re-raise to handle as normal failure
            raise

    def run(self):
        self.run_multithreaded()

    def run_multithreaded(self):
        """
        Continuously loop to check for and upload pending images.
        If meltdown is detected, we stop.
        If no images found, sleep for self.sleep_time seconds, then re-check.
        """

        while True:
            try:
                pending_images = self.db.fetch_pending_uploads(limit=50)

                if not pending_images:
                    logging.info(f"No pending images to upload. Sleeping {self.sleep_time} sec...")
                    time.sleep(self.sleep_time)
                    continue  # Then re-check in the next loop iteration

                logging.info(f"Found {len(pending_images)} images pending upload.")
                meltdown_detected = False

                # Use ThreadPoolExecutor for concurrent uploads
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_record = {
                        executor.submit(self.upload_image, record): record
                        for record in pending_images
                    }

                    for future in concurrent.futures.as_completed(future_to_record):
                        try:
                            future.result()  # Raises exception if meltdown or other error
                        except S3MeltdownError:
                            logging.warning("Meltdown detected, skipping remaining uploads.")
                            meltdown_detected = True
                            break
                        except Exception as e:
                            # Already handled in upload_image, but we can log or handle more if needed
                            pass

                if meltdown_detected:
                    logging.info("Exiting early due to meltdown.")
                    break  # End the while loop

            except Exception as e:
                logging.exception(f"An error occurred during the upload loop: {e}")
                # Decide if you want to break or keep going
                # We'll keep going to avoid stopping on a single random error
                pass

        # Once meltdown or another break condition, close connections
        self.db.close_all_connections()
        logging.info("Stopped continuous upload loop due to meltdown or shutdown.")


    def run_singlethreaded(self):
        """
        Continuously loop to check for and upload pending images in a single-threaded manner.
        If meltdown is detected, we stop.
        If no images found, sleep self.sleep_time seconds, then re-check.
        """

        while True:
            try:
                # Fetch up to 50 pending uploads
                pending_images = self.db.fetch_pending_uploads(limit=50)

                if not pending_images:
                    logging.info(f"No pending images to upload. Sleeping {self.sleep_time} seconds...")
                    time.sleep(self.sleep_time)
                    continue  # Then loop back to re-check

                logging.info(f"Found {len(pending_images)} images pending upload.")
                meltdown_detected = False

                # Single-thread: just iterate over the images
                for record in pending_images:
                    try:
                        self.upload_image(record)
                    except S3MeltdownError:
                        # A meltdown-level error means we skip further processing
                        logging.warning("Meltdown detected, stopping further uploads.")
                        meltdown_detected = True
                        break
                    except Exception as e:
                        # Already handled/logged in upload_image, but can optionally log more here
                        pass

                if meltdown_detected:
                    # If meltdown, break out of the while loop
                    logging.info("Exiting single-threaded loop early due to meltdown.")
                    break

            except Exception as e:
                logging.exception(f"An error occurred in the single-threaded loop: {e}")
                # You can decide whether to break or keep going. We keep going:
                pass

        # Once meltdown or other reason, close connections and log
        self.db.close_all_connections()
        logging.info("Stopped single-threaded continuous loop (meltdown or shutdown).")
