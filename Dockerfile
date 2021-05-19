FROM python:3.9.5

RUN mkdir -p /opt/iwxdatalineage

# Installing python dependencies
COPY requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt

# Copying src code to Container
COPY . /opt/iwxdatalineage