FROM golang:1.5
RUN go get github.com/hashicorp/raft && go get github.com/hashicorp/serf
WORKDIR /go/src/raftkv
COPY . /go/src/raftkv
