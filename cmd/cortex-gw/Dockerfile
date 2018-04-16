FROM debian:stretch-slim
LABEL author="Grafana Labs"

RUN mkdir -p /etc/gw/
COPY scripts/config/cortex-gw.ini /etc/gw/cortex-gw.ini
COPY scripts/entrypoint.sh /usr/bin/

# copy the built binaries from the build image
COPY build/cortex-gw /usr/bin/cortex-gw

EXPOSE 80
EXPOSE 443
EXPOSE 8001

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
CMD ["/usr/bin/cortex-gw", "-config=/etc/gw/cortex-gw.ini"]
