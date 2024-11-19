# syntax=docker/dockerfile:1

FROM golang:1.23

# Set destination for COPY
WORKDIR /build

# Download Go modules
COPY go.mod go.sum ./
COPY redis_ca.pem ./
COPY main.go ./

RUN go mod download

RUN GOOS=linux go build -o /conntest

ENTRYPOINT ["/conntest"]

CMD ["-a", "localhost:6379"]