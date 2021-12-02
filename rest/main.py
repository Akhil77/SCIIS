# main.py

from flask import Flask, Blueprint, render_template, request, Response, jsonify
import requests
from flask_login import login_required, current_user
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

# redis initialization
db = redis.Redis(host=redisHost, db=1)

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


main = Blueprint('main', __name__)

@main.route('/')
def index():
    return render_template('index.html')

@main.route('/upload-page')
@login_required
def upload_page():
    return render_template('upload.html', name=current_user.name)


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

@main.route('/upload-file', methods = ['POST'])
@login_required
def upload_file():
    try:
        if request.method == 'POST':
            file = request.files['file']
            img = file.read()
            image_path = file.filename
            username = current_user.name
            ext = image_path.split('.')[-1]

            # generating md5 id
            md5 = hashlib.md5(img)
            img_md5 = md5.hexdigest()

            # data construction to add to rabbitmq
            data = dict()
            data['documentId'] = img_md5
            data['filename'] = image_path
            data['username'] = username

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

            print("message added to rabbit mq")
            # adding status for publish
            data['status'] = 'success'

            print(data)
            
    except Exception as e:
        print(e)

    return render_template('upload.html', name=current_user.name)

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

@main.route('/preview/<md5>', methods = ['GET'])
def preview(md5):
    print(md5)
    fileFromBucket = download_blob_bytes(bucket_name, md5)
    print(type(fileFromBucket))
    response = { 'success' : 'Received md5 value'}
    # encode response using jsonpickle
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=200, mimetype="application/json")