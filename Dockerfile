FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/charon ./cmd/charon

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /bin/charon /usr/local/bin/charon
ENTRYPOINT ["/usr/local/bin/charon"]
