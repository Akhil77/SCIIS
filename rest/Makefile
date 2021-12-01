VERSION=v1
DOCKERUSER=viprabha

build:
	docker build -f Dockerfile -t sciis-rest .

push:
	docker tag sciis-rest $(DOCKERUSER)/sciis-rest:$(VERSION)
	docker push $(DOCKERUSER)/sciis-rest:$(VERSION)
	docker tag sciis-rest $(DOCKERUSER)/sciis-rest:latest
	docker push $(DOCKERUSER)/sciis-rest:latest
