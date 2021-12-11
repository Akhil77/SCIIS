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
bucket_name = 'final-proj-csci-5253'

# redis initialization
redisDocuments = redis.Redis(redisHost, db = 2)
redisKeys = redis.Redis(redisHost, db = 3)  


# mysql connection details
hostname = platform.node()
db_host = '10.41.224.9'
db_name = 'ocr_db'
db_user = 'root'
db_password = 'csci-password'

# establishing mysql connection
try:
    connection = mysql.connector.connect(host=db_host,
                                        database=db_name,
                                        user=db_user,
                                        password=db_password)
except mysql.connector.Error as error:
    print("Error Connecting to MySQL DB {}".format(error))

# Google application credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getcwd() + "/service-key.json"

# logger details
info = f"{platform.node()}.info"
debug = f"{platform.node()}.debug"

# debug logger function
def log_debug(message, key=debug):
    print("DEBUG:", message, file=sys.stderr)
    rabbitMQ = pika.BlockingConnection(
    pika.ConnectionParameters(host=rabbitMQHost))
    rabbitMQChannel = rabbitMQ.channel()
    rabbitMQChannel.exchange_declare(exchange='logs', exchange_type='topic')
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

@main.route('/search-page')
@login_required
def search():
    return render_template('search.html', name=current_user.name)


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

# endpoint to upload file to GCP
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
            
            print("Image uploaded to cloud storage", flush = True)

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

            print("message added to rabbit mq", flush = True)
            log_debug("Message added to rabbitmq", 'debug')
            # adding status for publish
            data['status'] = 'success'

            print(data)
            
    except Exception as e:
        print(e, flush = True)

    return render_template('upload.html', text="Image uploaded to Google Cloud, Upload more...")


# function to retrieve content from redis
def check_redis(username, key):
    redisKeys_query = username +":"+ key
    output = []
    response_redisKeys = ""
    response_redisDocuments = ""
    # getting the document ID for gvien keyword
    if redisKeys.get(redisKeys_query):
        response_redisKeys = json.loads(redisKeys.get(redisKeys_query).decode('utf-8'))\
    # getting the filename and safe search tags
    if response_redisKeys:
        for element in response_redisKeys:
            redisDocuments_query = username +":"+ element["documentId"]
            temp = dict()
            temp["md5"] = "https://storage.googleapis.com/" + bucket_name +"/" + element["documentId"]
            if redisDocuments.get(redisDocuments_query):
                response_redisDocuments = json.loads(redisDocuments.get(redisDocuments_query).decode('utf-8'))
                temp["file_name"] = response_redisDocuments[-1]["filename"]
                safe_search = response_redisDocuments[-2]
                safe_search_tag = ""
                for key, value in safe_search.items():
                    if value in ('POSSIBLE',
                       'LIKELY', 'VERY_LIKELY'):
                        safe_search_tag += key +', '
                temp["safe_search"] = safe_search_tag[0:len(safe_search_tag)-2]
            output.append(temp)
    return output
            
# function to retrieve content from MySQL
def check_mysql(keyword, username):
    print("Reading data from a doc table")
    binary_data = b''
    output = []
    try:
        cursor = connection.cursor()
        sql_query = """SELECT * from doc
                WHERE username = %s AND JSON_CONTAINS(labels,'{"description": """+ '"' + keyword + '"' + """}')"""
        get_blob_tuple = (username,)
        cursor.execute(sql_query, get_blob_tuple)
        record = cursor.fetchall()
        if record: 
            for element in record:
                temp = dict()
                temp["md5"] = "https://storage.googleapis.com/" + bucket_name +"/" + element[1]
                temp["file_name"] = element[-1]
                safe_search = json.loads(element[-2])
                safe_search_tag = ""
                for key, value in safe_search.items():
                    if value in ('POSSIBLE',
                       'LIKELY', 'VERY_LIKELY'):
                        safe_search_tag += key +', '
                temp["safe_search"] = safe_search_tag[0:len(safe_search_tag)-2]
                output.append(temp)
    
        return output
    except mysql.connector.Error as error:
        print("Failed to read BLOB data from MySQL table {}".format(error))
        binary_data = None


# endpoint to search for contexual keyword matched images
@main.route('/search-image', methods = ['POST'])
@login_required
def search_image():
    keyword = request.form.get('keyword')
    default_output = ""
    output = ""
    username = current_user.name
    if keyword:
        redis_output = check_redis(username, keyword)
        if redis_output:
            output = {'result':redis_output}
        else:
            sql_output = check_mysql(keyword, username)
            if sql_output:
                output = {'result':sql_output}
            else:
                log_debug("No images found by REST server", 'debug')
                default_output = "Could not find any images, try again ..."
    else:
        default_output = "Could not find any images, try again ..."
    return render_template('search.html', status=201, results=output, default_text = default_output)



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

# endpoint to retrieve image from GCP
@main.route('/preview/<md5>', methods = ['GET'])
def preview(md5):
    print(md5)
    fileFromBucket = download_blob_bytes(bucket_name, md5)
    print(type(fileFromBucket))
    response = { 'success' : 'Received md5 value'}
    # encode response using jsonpickle
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=200, mimetype="application/json")