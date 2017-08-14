FROM gliderlabs/alpine:3.6

USER root

COPY entrypoint.sh /
COPY script /home/script

RUN apk add --update --no-cache \
    python \
    python-dev \
    py-pip \
    bash \
    curl \
 && pip install --upgrade pip \
 && pip install es2csv \
 && chmod +x entrypoint.sh /home/script/csv_collector.py \
 && chmod a+r /home/script/config

ENTRYPOINT ["/entrypoint.sh"]