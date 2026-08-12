# AWS Quickstart Cheatsheet

Copy/paste commands for deploying to EKS. Replace placeholders
(`<account-id>`, `<region>`, `<cluster-name>`, etc.) with your own values.

## 1. Create ECR repository

```powershell
aws ecr create-repository --repository-name item-kafka-backend-express --region af-south-1
```

## 2. Build and push the image

```powershell
$ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text)
$REGION = "af-south-1"
$REPO = "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/item-kafka-backend-express"

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REPO

docker build -t item-kafka-backend-express .
docker tag item-kafka-backend-express:latest "${REPO}:latest"
docker push "${REPO}:latest"
```

## 3. Create an EKS cluster (skip if reusing one)

```powershell
eksctl create cluster --name item-kafka-poc-express --region af-south-1 --nodes 2 --node-type t3.medium
```

## 4. Configure kubectl

```powershell
aws eks update-kubeconfig --name item-kafka-poc-express --region af-south-1
```

## 5. Apply namespace, config, and secret

```powershell
kubectl apply -f k8s\namespace.yaml
kubectl apply -f k8s\backend-configmap.yaml

# Copy the example, fill in real values, then apply (do NOT commit backend-secret.yaml)
Copy-Item k8s\backend-secret.example.yaml k8s\backend-secret.yaml
notepad k8s\backend-secret.yaml
kubectl apply -f k8s\backend-secret.yaml
```

## 6. Update the image reference and deploy

Edit `k8s/backend-deployment.yaml` and replace the `image:` value with your
ECR repo URI from step 2, then:

```powershell
kubectl apply -f k8s\backend-deployment.yaml
kubectl apply -f k8s\backend-service.yaml
kubectl apply -f k8s\backend-hpa.yaml
```

## 7. Install the AWS Load Balancer Controller (once per cluster) and apply the Ingress

```powershell
# See https://docs.aws.amazon.com/eks/latest/userguide/aws-load-balancer-controller.html
kubectl apply -f k8s\backend-ingress-alb.yaml
```

## 8. Get the ALB hostname and validate

```powershell
kubectl get ingress -n item-kafka-poc-express
curl "http://<alb-hostname>/actuator/health"
```

## 9. Tail logs / debug

```powershell
kubectl get pods -n item-kafka-poc-express
kubectl logs -n item-kafka-poc-express deploy/item-kafka-backend -f
kubectl describe pod -n item-kafka-poc-express <pod-name>
```

## 10. Tear down

```powershell
kubectl delete -f k8s\backend-ingress-alb.yaml
kubectl delete -f k8s\backend-hpa.yaml
kubectl delete -f k8s\backend-service.yaml
kubectl delete -f k8s\backend-deployment.yaml
kubectl delete -f k8s\backend-secret.yaml
kubectl delete -f k8s\backend-configmap.yaml
kubectl delete -f k8s\namespace.yaml
eksctl delete cluster --name item-kafka-poc-express --region af-south-1
```

