FROM alpine
MAINTAINER Anthony Woods awoods@raintank.io

RUN apk --update add ca-certificates
RUN mkdir -p /etc/raintank
COPY config/tsdb.ini /etc/raintank/tsdb.ini

COPY build/tsdb-gw /usr/bin/tsdb-gw
COPY build/tsdb-usage /usr/bin/tsdb-usage
COPY entrypoint.sh /usr/bin/

EXPOSE 80
EXPOSE 443

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
CMD ["/usr/bin/tsdb-gw", "-config=/etc/raintank/tsdb.ini"]
