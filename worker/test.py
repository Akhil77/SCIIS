import io
import os
import json
import redis

# Imports the Google Cloud client library
from google.cloud import vision

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getcwd() + "/csci-5253-final-project-333308-ddebe3a38033.json"

resp = [{'description': 'dog', 'score': 0.8479596018791199}, {'description': 'carnivore', 'score': 0.885725200176239}, {'description': 'plant', 'score': 0.8846282362937927}, 
{'description': 'dog breed', 'score': 0.8721306920051575}, {'description': 'sunlight', 'score': 0.8429029583930969}]

resp1 = {'adult': 'VERY_UNLIKELY', 'medical': 'VERY_UNLIKELY', 'spoofed': 'VERY_UNLIKELY', 'violence': 'VERY_UNLIKELY', 'racy': 'VERY_UNLIKELY'}
resp.append(resp1)



username = 'akhil'
documentId = 'ee462de9b0e8d6781c3c18c55dbb84f7'
redisHost = os.getenv("REDIS_HOST") or "localhost"
redisAuthentication = redis.Redis(redisHost, db=1)  
redisDocuments = redis.Redis(redisHost, db = 2)
redisKeys = redis.Redis(redisHost, db = 3)

key = username + ":" + documentId
if not redisDocuments.exists(key):
    print('Doc doesnt exist')
    redisDocuments.set(key,json.dumps(resp))
respCheck = json.loads(redisDocuments.get(key))

for r in resp[:-1]:
    print(r)
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
        if match:
            val = sorted(val, key=lambda k: k['score'], reverse=True)
#respCheck = json.loads(redisDocuments.get(key))


#print(respCheck[-1])


'''
file_name = os.path.abspath('resources/dog.jpg')

    # Loads the image into memory
with io.open(file_name, 'rb') as image_file:
    fileFromBucket = image_file.read()
image = vision.Image(content=fileFromBucket)
client = vision.ImageAnnotatorClient()
response = client.label_detection(image=image, max_results = 5)
labels = response.label_annotations

resp = []
for label in labels:
    resp.append({'description':label.description, 'score':label.score})
print(resp)
resp = json.dumps(resp)

if response.error.message:
    raise Exception(
        '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(
                response.error.message))
'''
