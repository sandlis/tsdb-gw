ARG BASE_PATH=/go/src/github.com/raintank/tsdb-gw

# build binaries inside an alpine3.7 container
FROM golang:1.10.1-alpine3.7 AS build
ARG BASE_PATH

RUN apk --update add build-base linux-headers openssl-dev cyrus-sasl-dev bash python git

COPY . $BASE_PATH/
RUN $BASE_PATH/scripts/build.sh


# now build the production image based on the same alpine version
# as was used to build the binaries in
FROM alpine:3.7
ARG BASE_PATH

RUN apk --update add ca-certificates
RUN apk add openssl cyrus-sasl

RUN mkdir -p /etc/
COPY scripts/config/tsdb-gw.ini /etc/tsdb-gw.ini
COPY scripts/entrypoint.sh /usr/bin/

# copy the built binaries from the build image
COPY --from=build $BASE_PATH/build/tsdb-gw /usr/bin/tsdb-gw
COPY --from=build $BASE_PATH/build/tsdb-usage /usr/bin/tsdb-usage

EXPOSE 80
EXPOSE 443

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
CMD ["/usr/bin/tsdb-gw", "-config=/etc/tsdb-gw.ini"]
