ARG BASE_PATH=/go/src/github.com/raintank/tsdb-gw

# build binaries inside an alpine3.7 container
FROM golang:1.9.3-alpine3.7 AS build
ARG BASE_PATH

RUN apk --update add build-base linux-headers openssl-dev cyrus-sasl-dev bash python git
COPY . $BASE_PATH/
RUN $BASE_PATH/scripts/build.sh

# now build the production image based on the same alpine version
# as was used to build the binaries in
FROM alpine:3.7
LABEL author="Grafana Labs"
ARG BASE_PATH

RUN apk --update add ca-certificates
RUN apk add openssl cyrus-sasl

RUN mkdir -p /etc/gw/
COPY scripts/config/cortex-gw.ini /etc/gw/cortex-gw.ini
COPY scripts/entrypoint.sh /usr/bin/

# copy the built binaries from the build image
COPY --from=build $BASE_PATH/build/cortex-gw /usr/bin/cortex-gw

EXPOSE 80
EXPOSE 443
EXPOSE 8001

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
CMD ["/usr/bin/cortex-gw", "-config=/etc/gw/cortex-gw.ini"]
