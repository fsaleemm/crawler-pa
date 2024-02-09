# To enable ssh & remote debugging on app service change the base image to the one below
# FROM mcr.microsoft.com/azure-functions/python:4-python3.11-appservice
FROM mcr.microsoft.com/azure-functions/python:4-python3.11

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

# Install unzip and Chrome
ARG CHROME_VERSION="google-chrome-stable"
RUN apt-get update && apt-get install -y unzip wget \
  && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
  && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
  && apt-get update -qqy \
  && apt-get -qqy install ${CHROME_VERSION:-google-chrome-stable} \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /var/cache/apt/* /etc/apt/sources.list.d/google-chrome.list

# Install Python dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY . /home/site/wwwroot