# To enable ssh & remote debugging on app service change the base image to the one below
# FROM mcr.microsoft.com/azure-functions/python:4-python3.11-appservice
FROM mcr.microsoft.com/azure-functions/python:4-python3.11

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

# Install unzip, Chrome and ChromeDriver
ARG CHROME_VERSION="google-chrome-stable"
RUN apt-get update && apt-get install -y unzip wget curl \
  && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
  && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
  && apt-get update -qqy \
  && apt-get -qqy install ${CHROME_VERSION:-google-chrome-stable} \
  && CHROME_DRIVER_VERSION=$(curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE) \
  && wget -N http://chromedriver.storage.googleapis.com/$CHROME_DRIVER_VERSION/chromedriver_linux64.zip -P /tmp/ \
  && unzip /tmp/chromedriver_linux64.zip -d /tmp/ \
  && mv /tmp/chromedriver /usr/local/bin/chromedriver \
  && chmod +x /usr/local/bin/chromedriver \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /var/cache/apt/* /etc/apt/sources.list.d/google-chrome.list /tmp/chromedriver_linux64.zip

# Set Chrome environment variables
ENV CHROME_BIN=/usr/bin/google-chrome
ENV CHROME_PATH=/usr/bin/google-chrome

# Install Python dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY . /home/site/wwwroot