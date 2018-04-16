FROM debian:stretch-slim

RUN apt-get update
RUN apt-get install -y libssl1.1 libsasl2-2

RUN mkdir -p /etc/gw/
COPY scripts/config/tsdb-gw.ini /etc/gw/tsdb-gw.ini
COPY scripts/entrypoint.sh /usr/bin/

# copy the built binaries from the build image
COPY build/tsdb-gw /usr/bin/tsdb-gw
COPY build/tsdb-usage /usr/bin/tsdb-usage

EXPOSE 80
EXPOSE 443

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
CMD ["/usr/bin/tsdb-gw", "-config=/etc/gw/tsdb-gw.ini"]
