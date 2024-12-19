#!/bin/bash

# Set environment variables
export KUBECTL_VERSION="1.21.0"
export AWS_REGION="us-east-1"
export CLUSTER_NAME="agnes-cluster"

# Install kubectl
curl -LO "https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin/

# Configure kubectl
aws eks update-kubeconfig --region $AWS_REGION --name $CLUSTER_NAME

# Create namespace if it doesn't exist
kubectl create namespace agnes --dry-run=client -o yaml | kubectl apply -f -

# Apply ConfigMaps
kubectl apply -f kubernetes/configmaps/

# Apply Secrets
kubectl apply -f kubernetes/secrets/

# Deploy applications
kubectl apply -f kubernetes/deployment.yaml

# Wait for deployment to complete
kubectl rollout status deployment/agnes -n agnes

# Check deployment status
kubectl get all -n agnes
