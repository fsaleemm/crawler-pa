# Web Crawler Function

## Prerequisites

1. Azure Container Registry
1. Azure Function App
1. Docker Desktop Installed

## Docker Image Deployment

Login to Azure Container Registry

```ps
az acr login --name <azure-container-registry-name>
```

Build the container image

```ps
docker build --tag f4t-crawler:v1.0.0 .
```

Prepare to push to Azure Container Registry

```ps
docker tag f4t-crawler:v1.0.0 <azure-container-registry-name>.azurecr.io/f4t-crawler:v1.0.0
```

Push to Azure Container Registry

```ps
docker push <azure-container-registry-name>.azurecr.io/f4t-crawler:v1.0.0
```

