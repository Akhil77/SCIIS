#
# Worker server
#
import jsonpickle
import platform
import io
import os
import sys
import pika
import redis
import json
import mysql.connector
from mysql.connector import Error
from google.cloud import storage
# Imports the Google Cloud client library
from google.cloud import vision

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getcwd() + "/service-key.json"

hostname = platform.node()
db_host = '10.41.224.3'
db_name = 'ocr_db'
db_user = 'root'
db_password = 'csci-password'

try:
    connectionDB = mysql.connector.connect(host=db_host,
                                        database=db_name,
                                        user=db_user,
                                        password=db_password)
except mysql.connector.Error as error:
    print("Error Connecting to MySQL DB {}".format(error))

redisHost = os.getenv("REDIS_HOST") or "localhost"
rabbitMQHost = os.getenv("RABBITMQ_HOST") or "localhost"

print(f"Connecting to rabbitmq({rabbitMQHost}) and redis({redisHost})")

##
## Set up redis connections
##
redisAuthentication = redis.Redis(redisHost, db=1)  
redisDocuments = redis.Redis(redisHost, db = 2)
redisKeys = redis.Redis(redisHost, db = 3)                                                                       
##
## Set up rabbitmq connection
##
rabbitMQ = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitMQHost))
rabbitMQChannel = rabbitMQ.channel()

rabbitMQChannel.exchange_declare(exchange='toWorker', exchange_type='direct')
result = rabbitMQChannel.queue_declare(queue='toWorker')
queue_name = result.method.queue

rabbitMQChannel.queue_bind(
        exchange='toWorker', queue=queue_name, routing_key="toWorker")



rabbitMQChannel.exchange_declare(exchange='logs', exchange_type='topic')
infoKey = f"{platform.node()}.worker.info"
debugKey = f"{platform.node()}.worker.debug"
def log_debug(message, key=debugKey):
    print("DEBUG:", message, file=sys.stdout)
    rabbitMQChannel.basic_publish(
        exchange='logs', routing_key=key, body=message)
def log_info(message, key=infoKey):
    print("INFO:", message, file=sys.stdout)
    rabbitMQChannel.basic_publish(
        exchange='logs', routing_key=key, body=message)


log_debug(queue_name, 'name')

##
## Your code goes here...
##

def download_blob_bytes(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    bytes_data = blob.download_as_string()

    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        'blob bytes data'))

    return bytes_data

def vision_api(fileBytes):
    # check if file or document and then perform OCR on it.
    image = vision.Image(content=fileBytes)
    client = vision.ImageAnnotatorClient()

    response = client.label_detection(image=image, max_results = 5)
    labels = response.label_annotations

    resp = []
    for label in labels:
        resp.append({'description':label.description.lower(), 'score':label.score})

    if response.error.message:
        raise Exception(
            '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(
                response.error.message))

    response = client.safe_search_detection(image=image)
    safe = response.safe_search_annotation
    # Names of likelihood from google.cloud.vision.enums
    likelihood_name = ('UNKNOWN', 'VERY_UNLIKELY', 'UNLIKELY', 'POSSIBLE',
                       'LIKELY', 'VERY_LIKELY')
    resp1 = {}
    resp1['adult'] = likelihood_name[safe.adult]
    resp1['medical'] = likelihood_name[safe.medical]
    resp1['spoofed'] = likelihood_name[safe.spoof]
    resp1['violence'] = likelihood_name[safe.violence]
    resp1['racy'] = likelihood_name[safe.racy]
    resp2 = resp.copy()
    resp2.append(resp1)
    resp = json.dumps(resp)
    resp1 = json.dumps(resp1)

    if response.error.message:
        raise Exception(
            '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(
                response.error.message))

    return resp,resp1,resp2

def storeContentInSql(username, documentId, labelObj, safeSearchObj):
    try:
        cursor = connectionDB.cursor()
        sql_insert_blob_query = """ INSERT INTO doc
                          (username, documentId, labels, safeSearch) VALUES (%s, %s, %s, %s)"""

        insert_text_tuple = (username, documentId, labelObj, safeSearchObj)
        result = cursor.execute(sql_insert_blob_query, insert_text_tuple)
        connectionDB.commit()
        print(username, documentId, labelObj)
    except mysql.connector.Error as error:
        print("Failed inserting data into MySQL table {}".format(error))
    finally:
        cursor.close()

def workerCallback(ch, method, properties, body):
    print(" [Y] Received %r" % "Data to Worker " + hostname + ":" + method.routing_key)
    data = jsonpickle.decode(body)
    print(data) # data will contain document/image id, filename and username
    username = data['username']
    documentId = data['documentId']
    bucket_name = 'final-proj-csci-5253'

    # get the file from bucket
    fileFromBucket = download_blob_bytes(bucket_name, data['documentId'])

    print("Fetched file from bucket")
    #file_name = os.path.abspath('resources/dog.jpg')

    # Loads the image into memory
    #with io.open(file_name, 'rb') as image_file:
    #    fileFromBucket = image_file.read()

    # perform OCR on the file and store results in SQL
    LabelObj, SafeSearchObj, redisStoreObj = vision_api(fileFromBucket)
    print("Vision extracted")

    storeContentInSql(data['username'], data['documentId'], LabelObj, SafeSearchObj)

    print("Stored in sql", flush= True)

    # Classify the text obtained from document and store results
    key = username + ":" + documentId
    if not redisDocuments.exists(key):
        print('Doc doesnt exist')
        redisDocuments.set(key,json.dumps(redisStoreObj))

    for r in redisStoreObj[:-1]:
        key = username+":"+r['description']
        if not redisKeys.exists(key):
            print('Key doesnt exist')
            val = []
            val.append({'documentId': documentId, 'score':r['score']})
            redisKeys.set(key,json.dumps(val))
        else:
            val = json.loads(redisKeys.get(key))
            match = False
            for v in val:
                if v['documentId'] == documentId:
                    match= True
                    break
            if not match:
                val.append({'documentId': documentId, 'score':r['score']})
                redisKeys.set(key,json.dumps(val))
            val = sorted(val, key=lambda k: k['score'], reverse=True)# For sorting on rest server
    

rabbitMQChannel.basic_consume('toWorker', workerCallback, auto_ack=True)
try:
    rabbitMQChannel.start_consuming()
except KeyboardInterrupt:
    rabbitMQChannel.stop_consuming()
rabbitMQChannel.close()