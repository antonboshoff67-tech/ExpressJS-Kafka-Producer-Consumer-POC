# AWS - Start Here

Orientation for deploying `ExpressJS-Kafka-Producer-Consumer-POC` to AWS.

## What you'll deploy

| AWS service | Role |
|---|---|
| **ECR** | Hosts the built Docker image (`Dockerfile` at repo root) |
| **EKS** | Runs the Express.js backend as a Kubernetes Deployment (see `k8s/`) |
| **RDS for MySQL** | Source + sink databases (or reuse one instance, two databases) |
| **MSK (Managed Kafka)** | Kafka broker, or point at Confluent Cloud instead |
| **ALB Ingress** | Public HTTPS entry point (`k8s/backend-ingress-alb.yaml`) |

## Prerequisites

- AWS CLI configured (`aws configure`)
- `kubectl`
- `eksctl` (or Terraform/CloudFormation if you manage infra that way)
- Docker

## High-level flow

1. Create (or reuse) an EKS cluster.
2. Create an ECR repository and push the built image.
3. Provision RDS MySQL (source + sink databases) and run the scripts in `sql-scripts/`.
4. Provision MSK (or use Confluent Cloud) and note the bootstrap servers.
5. Fill in `k8s/backend-configmap.yaml` and a copy of `k8s/backend-secret.example.yaml` (as `backend-secret.yaml`, **not committed**) with your real endpoints/credentials.
6. Apply the manifests in `k8s/` (namespace → configmap/secret → deployment → service → hpa → ingress).
7. Point your DNS record at the ALB Ingress hostname.
8. Validate `https://your-api-domain/actuator/health` returns `{"status":"UP"}`.

See `AWS_QUICKSTART_CHEATSHEET.md` for the exact copy/paste commands and
`EKS_README.md` for the fully detailed walkthrough.

## Cost note

Every AWS resource above (EKS, RDS, MSK, ALB, ECR storage/data transfer) is a
billable AWS service. This project does not use any paid third-party API or
bundled paid asset itself, but running it on AWS will incur your own AWS
account's usage charges - review the AWS pricing pages for each service
before deploying.

