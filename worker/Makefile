VERSION=v6
DOCKERUSER=akhilsm

build:
	docker build -f Dockerfile -t final-worker .
push:
	docker tag final-worker $(DOCKERUSER)/final-worker:$(VERSION)
	docker push $(DOCKERUSER)/final-worker:$(VERSION)
	docker tag final-worker $(DOCKERUSER)/final-worker:latest
	docker push $(DOCKERUSER)/final-worker:latest

#kubectl create deployment worker --image=gcr.io/dcsc-3/worker