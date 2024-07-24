# Retrieve custom metrics from OCI Monitoring and send to OCI Streaming

## Implemented using the Python SDK with the intention of running on Kubernetes
Current scope is intended to be `gpu_infrastructure_health` and `rdma_infrastructure_health` metrics namespaces. Assumes container will run on a pod in OKE with access to a resource principal for authN and authZ.