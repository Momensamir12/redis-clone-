FROM alpine:3.19

RUN apk add --no-cache \
    build-base \
    cmake \
    redis \
    bash

WORKDIR /app

COPY src/ ./src/
COPY CMakeLists.txt .
COPY run.sh .
COPY vcpkg.json .
COPY vcpkg-configuration.json .

RUN chmod +x run.sh


EXPOSE 6379
CMD ["./run.sh"]