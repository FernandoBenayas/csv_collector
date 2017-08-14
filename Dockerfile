FROM gliderlabs/alpine:3.6

USER root

COPY entrypoint.sh /
COPY script /home/script

RUN apk add --update --no-cache \
    python \
    python-dev \
    py-pip \
    bash \
 && pip install --upgrade pip \
 && pip install es2csv \
 && chmod +x entrypoint.sh /home/script/csv_collector.py

ENTRYPOINT ["/entrypoint.sh"]