FROM ubuntu:xenial
RUN apt-get update &&\
    apt-get install -y npm python3-pip python3-dev &&\
    ln -snf /usr/bin/python3 /usr/local/bin/python &&\
    ln -snf /usr/bin/nodejs /usr/bin/node &&\
    pip3 install --upgrade pip

RUN useradd -ms /bin/bash restrabbit

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt


COPY . .
RUN npm install &&\
    chown restrabbit:restrabbit -R .

USER restrabbit

CMD ["python3", "service.py"]
