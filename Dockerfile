FROM ubuntu:xenial
RUN apt-get update &&\
    apt-get install -y python3-pip python3-dev &&\
    ln -snf /usr/bin/python3 /usr/local/bin/python &&\
    pip3 install --upgrade pip

RUN useradd -ms /bin/bash restrabbit

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

USER restrabbit

CMD ["python3", "service.py"]
