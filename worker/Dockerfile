FROM python:3.6-slim

RUN apt-get update

RUN pip3 install --upgrade pika
RUN pip3 install jsonpickle
RUN pip3 install pickle-mixin
RUN pip3 install pillow
RUN pip3 install redis
RUN pip3 install google-cloud-storage
RUN pip3 install mysql-connector-python
RUN pip3 install --upgrade google-cloud-language
RUN pip3 install --upgrade google-cloud-vision


WORKDIR /server

COPY worker-server.py /server/
COPY service-key.json /server/
COPY resources/dog.jpg /server/resources/dog.jpg

CMD ["ENV" ,"PYTHONUNBUFFERED=0"]
CMD ["export","GOOGLE_APPLICATION_CREDENTIALS='./service-key.json'"]
CMD ["python3", "worker-server.py"]