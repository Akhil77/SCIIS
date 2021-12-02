FROM python:3.6-slim

WORKDIR /server

COPY . /server/

# COPY templates /server/

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "run.py"]