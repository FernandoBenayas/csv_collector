FROM ubuntu:16.04

USER root

COPY entrypoint.sh /
COPY script /root/script

RUN apt-get update && apt-get install -y --no-install-recommends \
    python \
    python-dev \
    python-pip \
    python-setuptools \
    bash \
    curl \
    vim \
 && pip install --upgrade pip \
 && pip install es2csv \
 && pip install numpy \
 && pip install pandas \
 && chmod +x entrypoint.sh /root/script/csv_collector.py \
 && chmod a+r /root/script/config

ENTRYPOINT ["/entrypoint.sh"]