FROM golang:alpine as build_container
WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go build -o task-queue ./cmd/server/main.go

FROM alpine
COPY --from=build_container /app/task-queue /usr/bin
ARG GRPC_PORT
EXPOSE $GRPC_PORT
ENTRYPOINT ["task-queue"]