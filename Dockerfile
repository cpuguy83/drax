FROM golang:1.5
RUN go get github.com/hashicorp/raft && \
  go get github.com/hashicorp/serf && \
  go get github.com/sirupsen/logrus && \
  go get github.com/docker/docker/pkg/signal && \
  go get github.com/boltdb/bolt && \
  go get github.com/docker/libkv/store && \
  go get github.com/hashicorp/raft-boltdb

COPY . /go/src/github.com/cpuguy83/drax
WORKDIR /go/src/github.com/cpuguy83/drax
