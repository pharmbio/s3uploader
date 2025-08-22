# s3uploader
service that uploads images to s3

s3_image_uploader.py contains the main S3ImageUploader class
<br>
main_uploader.py is creating the S3ImageUploader class and call the run method
<br>
S3ImageUploader run method is:
<br>
- polling database every 30 sec to if there are new entries in the database table "upload_to_s3"
- if there are new images create a s3 boto client (wrapped in s3_client_wrapper, to make sure there is a fresh s3 token always)
- upload file/image to s3
- remove file/image from database
<br>
there is also a Dockerfile and a docker-compose file for running the service in docker with docker-compose
