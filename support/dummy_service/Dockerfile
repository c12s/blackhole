FROM golang:alpine as build_container
WORKDIR /app
COPY ../../go.mod .
COPY ../../go.sum .
RUN go mod download
COPY . .
RUN go run ./dumy_service.go
