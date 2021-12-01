#
# Worker server
#
import pickle
import platform
import io
import os
import sys
import pika
import redis
import hashlib
import json
import requests
import jsonpickle

from flair.models import TextClassifier
from flair.data import Sentence


hostname = platform.node()

##
## Configure test vs. production
##
redisHost = os.getenv("REDIS_HOST") or "localhost"
rabbitMQHost = os.getenv("RABBITMQ_HOST") or "localhost"

print(f"Connecting to rabbitmq({rabbitMQHost}) and redis({redisHost})")

##
## Set up redis connections
##


db = redis.Redis(host=redisHost, db=1)                                                                           


def workerCallback(ch, method, properties, body):
    print("Received Data to Worker " + hostname + ":" + method.routing_key)
    #log_info("Received Data to Worker " + hostname + ":" + method.routing_key)
    data = jsonpickle.decode(body)
    model = data['model']
    classifier = TextClassifier.load(model)
    for sentence in data['sentences']:
        key = model +':'+ sentence
        if db.get(key):
            print("found sentence in redis")
            #log_info("Found sentence in redis")
            # print(db.get(key).decode('utf-8'))
        else:
            sentence = Sentence(sentence)
            classifier.predict(sentence)
            result = json.dumps(sentence.to_dict('sentiment'))
            print("Finding sentiment for new sentence and adding it to redis")
            #log_info("Finding sentiment for new sentence and adding it to redis")
            db.set(key, result)


##
## Set up rabbitmq connection
##
rabbitMQ = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitMQHost))
rabbitMQChannel = rabbitMQ.channel()

# rabbitMQChannel.queue_declare(queue='toWorker')
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


##
## Your code goes here...
##
result = rabbitMQChannel.queue_declare(queue='toWorker')
rabbitMQChannel.exchange_declare(exchange='toWorker', exchange_type='direct')
queue_name = result.method.queue

rabbitMQChannel.queue_bind(
        exchange='toWorker', queue=queue_name, routing_key="toWorker")

rabbitMQChannel.basic_consume(
    queue=queue_name, on_message_callback=workerCallback, auto_ack=True)

rabbitMQChannel.start_consuming()


 
