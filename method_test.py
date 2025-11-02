import logging
import sys
import os
from dotenv import load_dotenv

from s3_image_uploader import S3ImageUploader
from database import Database

def setup_logging():
    # Configure root logger to DEBUG (so your code sees debug logs).
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)-8s [%(filename)-30s:%(lineno)4d] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout
    )

    # Silence or reduce logs from external libraries
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    # If you use paramiko or other libs that can be chatty:
    # logging.getLogger("paramiko").setLevel(logging.WARNING)


def main():
    setup_logging()

    load_dotenv()

    db = Database.get_instance()
    db_config = {
        "user": os.getenv('DB_USER'),
        "password": os.getenv('DB_PASS'),
        "host": os.getenv('DB_HOSTNAME'),
        "port": os.getenv('DB_PORT'),
        "database": os.getenv('DB_NAME')
    }
    db.initialize_connection_pool


    # S3 configuration
    s3_config = {
        'endpoint_url': os.getenv('ENDPOINT_URL'),
    }

    # Initialize the uploader
    uploader = S3ImageUploader(db_config=db_config, s3_config=s3_config, max_workers=5)

    # test data
    test_image_record = {
                "id": -1,
                "image_id": -1,
                "acq_id": -1,
                "local_path": "/share/mikro2/squid/anders-test/Testplate_monitor_2023-04-18_14.16.04/A03_s1_x0_y0_BF_LED_matrix_full.tiff"
    }
    test_bucket = "mikro"

    #uploader.delete_image(test_image_record, test_bucket)

    #uploader.upload_image(test_image_record, test_bucket)

    logging.info(f"image_record: {test_image_record}")


    ## delete record in db
    #db.delete_image_from_imagedb()

if __name__ == "__main__":
    main()






