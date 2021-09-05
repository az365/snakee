FROM python:3.6

WORKDIR /

COPY requirements.txt /

RUN apt-get -y update

# Install dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt
