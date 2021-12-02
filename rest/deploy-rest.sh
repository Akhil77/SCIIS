kubectl apply -f rest-deployment.yaml
kubectl apply -f rest-service.yaml

sleep 5

kubectl apply -f ingress.yaml