FROM alpine
MAINTAINER Anthony Woods awoods@raintank.io

RUN apk --update add ca-certificates
RUN mkdir -p /etc/raintank
COPY config/tsdb.ini /etc/raintank/tsdb.ini

COPY build/tsdb-gw /usr/bin/tsdb-gw
COPY entrypoint.sh /usr/bin/

EXPOSE 80
EXPOSE 443

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
CMD ["-config=/etc/raintank/tsdb.ini"]
