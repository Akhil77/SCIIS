##
from flask import Flask, request, Response, jsonify
import platform
import io, os, sys
import pika, redis
import hashlib, requests
import json
import jsonpickle
from PIL import Image

# Google cloud storage bucket library
from google.cloud import storage

# MySQL library
import mysql.connector
from mysql.connector import Error


# redis and rabbitmq configuration
redisHost = os.getenv("REDIS_HOST") or "localhost"
rabbitMQHost = os.getenv("RABBITMQ_HOST") or "localhost"
print("Connecting to rabbitmq({}) and redis({})".format(rabbitMQHost,redisHost))

# storage bucket name
bucket_name = 'dcsc-final-project-bucket'

# Google application credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getcwd() + "/dcsc-project.json"

# logger details
info = f"{platform.node()}.info"
debug = f"{platform.node()}.debug"

# debug logger function
def log_debug(message, key=debug):
    print("DEBUG:", message, file=sys.stderr)
    rabbitMQ = pika.BlockingConnection(
    pika.ConnectionParameters(host=rabbitMQHost))
    rabbitMQChannel = rabbitMQ.channel()
    rabbitMQChannel.basic_publish(
        exchange='logs', routing_key=key, body=message)
    rabbitMQChannel.close()

# info logger function
def log_info(message, key=info):
    print("INFO:", message, file=sys.stdout)
    rabbitMQ = pika.BlockingConnection(
    pika.ConnectionParameters(host=rabbitMQHost))
    rabbitMQChannel = rabbitMQ.channel()
    rabbitMQChannel.exchange_declare(exchange='logs', exchange_type='topic')
    rabbitMQChannel.basic_publish(
        exchange='logs', routing_key=key, body=message)
    rabbitMQChannel.close()


# Initialize the Flask application
app = Flask(__name__)


# redis initialization
db = redis.Redis(host=redisHost, db=1)

# function to upload image as bytes into Google Cloud Storage bucket
def upload_blob_bytes(bucket_name, source_bytes, destination_blob_name, ext):
    # setting the file extension
    if(ext == 'jpg' or ext == 'jpeg' or ext == 'png'):
        ext = 'image/jpeg'
    elif(ext == 'pdf'):
        ext = 'application/pdf'
    else:
        ext = 'text/plain'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(source_bytes, content_type=ext)

    print('Data {} uploaded to {}.'.format(
        'source_bytes',
        destination_blob_name))

# upload endpoint to push image to cloud storage and rabbitmq
@app.route('/upload', methods=['POST'])
def upload():
 try:
  # dummy file data
  image_path = "dog.jpeg"
  ext = image_path.split('.')[-1]
  username = "testUser"
  fileDescription = "image of a cat"
  
  # reading image as bytes
  with io.open(image_path, 'rb') as image_file:
    img = image_file.read()

  # generating md5 id
  md5 = hashlib.md5(img)
  img_md5 = md5.hexdigest()

  # data construction to add to rabbitmq
  data = dict()
  data['documentId'] = img_md5
  data['filename'] = image_path
  data['username'] = username
  data['fileDescription'] = fileDescription

  print(data)

  # store file in google cloud bucket.
  upload_blob_bytes(bucket_name, img, img_md5, ext)
  
  print("Image uploaded to cloud storage")
  
  # rabbitmq publish
  connection = pika.BlockingConnection(
  pika.ConnectionParameters(host=rabbitMQHost))
  channel = connection.channel()
  channel.exchange_declare(exchange='toWorker', exchange_type='direct')
  channel.basic_publish(
            exchange='toWorker',
            routing_key='toWorker',
            body=jsonpickle.encode(data))
  channel.close()

  # adding status for publish
  data['status'] = 'success'
  response = data
 except Exception as e:
   print(e)
   response = { 'error' : 'Could not process request'}
 
 # encode response using jsonpickle
 response_pickled = jsonpickle.encode(response)
 return Response(response=response_pickled, status=201, mimetype="application/json")

# function to download image from Google Cloud Storage bucket using md5 value
def download_blob_bytes(bucket_name, source_blob_name):
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    bytes_data = blob.download_as_string()

    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        'blob bytes data'))

    return bytes_data

# preview endpoint to retrieve the image from cloud storage and preview
@app.route('/preview/<md5>', methods=['GET'])
def preview(md5):
  print(md5)
  fileFromBucket = download_blob_bytes(bucket_name, md5)
  print(type(fileFromBucket))
  image = Image.open(io.BytesIO(fileFromBucket))
  image.show()
  response = { 'success' : 'Received md5 value'}
  # encode response using jsonpickle
  response_pickled = jsonpickle.encode(response)
  return Response(response=response_pickled, status=200, mimetype="application/json")


# @app.route('/apiv1/cache/sentiment', methods=['GET'])
# def retrieveRedis():
#  try:
#   log_info("Received action to dump cache")
#   response = dict()
#   keys = db.keys("*")
#   for key in keys:
#     response[key.decode('utf-8')] = db.get(key).decode('utf-8')
  
#   log_info("Cache dump sent back")


#  except Exception as e:
#    print(e)
#    response = { 'error' : 'could not retriece anything from redis cache'}
#     # encode response using jsonpickle
#  response_pickled = jsonpickle.encode(response)
#  return Response(response=response_pickled, status=200, mimetype="application/json")



# @app.route('/apiv1/sentiment/sentence', methods=['GET'])
# def retrieveSentences():
#  try:
#   data = request.json

#   response = []
#   model = data['model']
#   for sentence in data['sentences']:
#     key = model +':'+ sentence
#     if db.get(key):
#      response.append(db.get(key).decode('utf-8'))

#  except Exception as e:
#    print(e)
#    response = { 'error' : 'could not retriece sentence analysis from redis cache'}
#     # encode response using jsonpickle
#  response_pickled = jsonpickle.encode({"sentences": response})
#  return Response(response=response_pickled, status=200, mimetype="application/json")

# start flask app
if __name__ == '__main__':
  app.run(host="0.0.0.0", port=5000)
