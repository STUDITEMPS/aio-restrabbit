FROM ubuntu:xenial
RUN apt-get update \
    && apt-get install -y python3-pip python3-dev \
    && cd /usr/local/bin \
    && ln -s /usr/bin/python3 python \
    && pip3 install --upgrade pip \
    && mkdir -p /opt/aio-restrabbit/

ADD . /opt/aio-restrabbit/
WORKDIR /opt/aio-restrabbit/
RUN pip3 install -r requirements.txt
CMD ["python3","service.py"]
