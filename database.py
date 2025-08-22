import logging
import psycopg2
import threading
from typing import List, Optional
from psycopg2.extras import RealDictCursor
from psycopg2 import pool

class Database:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self.connection_pool = None

    @staticmethod
    def get_instance():
        if Database._instance is None:
            with Database._lock:
                if Database._instance is None:
                    Database._instance = Database()
        return Database._instance

    def initialize_connection_pool(self, user: str, password: str, host: str, port: str, database: str):
        """
        Initializes the PostgreSQL connection pool.
        """
        try:
            self.connection_pool = pool.ThreadedConnectionPool(
                1, 20,
                user=user,
                password=password,
                host=host,
                port=port,
                database=database
            )
            logging.info("Database connection pool initialized.")
        except Exception as e:
            logging.exception("Failed to initialize database connection pool.")
            raise e

    def get_connection(self):
        if self.connection_pool:
            return self.connection_pool.getconn()
        else:
            raise Exception("Connection pool is not initialized.")

    def release_connection(self, conn):
        if self.connection_pool:
            self.connection_pool.putconn(conn)

    def close_all_connections(self):
        if self.connection_pool:
            self.connection_pool.closeall()

    def execute_query(self, query: str, params: Optional[tuple] = None,
                      commit: bool = False, fetch: bool = True):
        """
        A helper method to execute SQL queries.
        """
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if commit:
                    conn.commit()
                if fetch:
                    return cursor.fetchall()
                else:
                    return None
        except Exception as e:
            logging.exception("Failed to execute query.")
            conn.rollback()
            raise e
        finally:
            self.release_connection(conn)

    def fetch_pending_uploads(self, limit: int = 50):
        """
        Fetch rows from 'upload_to_s3' that have status='pending' and retry_count < 5.
        Returns up to 'limit' rows.
        """
        query_fetch = """
            SELECT
                id,
                image_id,
                acq_id,
                path AS local_path
            FROM upload_to_s3
            WHERE retry_count < 5
            LIMIT %s
        """
        return self.execute_query(query_fetch, params=(limit,))

    def fetch_pending_uploads_single_image(self, path: str):
        pending_images = self.fetch_pending_uploads()
        if not pending_images:
            logging.info("No pending images to upload.")
            return

        for record in pending_images:
            if record['local_path'] == path:
                return record

        return

    def delete_uploaded_record(self, upload_id: int):
        """
        Delete a record from 'upload_to_s3' after a successful upload.
        """
        query_delete = """
            DELETE FROM upload_to_s3
            WHERE id = %s
        """
        self.execute_query(query_delete, params=(upload_id,), commit=True, fetch=False)
        logging.info(f"Deleted record with ID {upload_id} from 'upload_to_s3'.")

    def mark_as_failed(self, upload_id: int, error_msg: str):
        """
        Update a record to 'failed' status, store the last error message,
        and increment the retry_count column by 1.
        """
        query_update = """
            UPDATE upload_to_s3
            SET status = 'failed',
                last_error = %s,
                retry_count = retry_count + 1
            WHERE id = %s
        """
        self.execute_query(query_update, params=(error_msg, upload_id), commit=True, fetch=False)
        logging.info(f"Marked upload ID {upload_id} as 'failed' (incremented retry_count), error: {error_msg}")

    def insert_into_uploaded_s3(self, image_id: int, acq_id: int, local_path: str, s3_path: str, bucket_name: str):
        """
        Insert a record into 'uploaded_s3' to keep track of successful S3 uploads.
        """
        query = """
            INSERT INTO uploaded_s3 (image_id, acq_id, path, object_key, bucket)
            VALUES (%s, %s, %s, %s, %s)
        """
        params = [image_id, acq_id, local_path, s3_path, bucket_name]
        self.execute_query(query, params=params, commit=True, fetch=False)
        logging.info(f"Inserted record into 'uploaded_s3' for local_path={local_path}")

    def delete_image_from_imagedb(self, path: int):
        """
        Delete a record from 'images' for debugging purposes
        """
        query_delete = """
            DELETE FROM images
            WHERE path = %s
        """
        self.execute_query(query_delete, params=(path,), commit=True, fetch=False)
        logging.info(f"Deleted from images: {path}")

