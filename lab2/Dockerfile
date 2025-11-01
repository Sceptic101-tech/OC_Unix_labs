FROM ubuntu:latest

RUN apt-get update && apt-get install -y util-linux

WORKDIR /app

COPY container_internals.sh .

RUN chmod +x container_internals.sh

CMD ["/bin/sh", "container_internals.sh"]