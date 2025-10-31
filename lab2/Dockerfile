FROM alpine:latest

RUN apk add --no-cache util-linux

WORKDIR /app

COPY container_internals.sh .

RUN chmod +x container_internals.sh

CMD ["/bin/sh", "container_internals.sh"]