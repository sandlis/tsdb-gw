FROM alpine

RUN mkdir -p /etc/gw

COPY build/persister-gw /usr/bin/persister-gw
COPY scripts/entrypoint.sh /usr/bin/

EXPOSE 80 
EXPOSE 443
EXPOSE 8001

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
CMD ["/usr/bin/persister-gw", "-config=/etc/gw/persister-gw.ini"]