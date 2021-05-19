FROM python:3.9.5

RUN mkdir -p /opt/iwxdatalineage

# Installing python dependencies
COPY requirements.txt /opt/iwxdatalineage
RUN pip install --no-cache-dir -r /opt/iwxdatalineage/requirements.txt

# Copying src code to Container
COPY . /opt/iwxdatalineage