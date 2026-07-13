# Build localgcp (including the Cloud Run Jobs API) from source.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /localgcp ./cmd/localgcp

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /localgcp /usr/local/bin/localgcp
# Cloud Storage 4443 · Pub/Sub 8085 · Cloud Run (+ Jobs) 8093
EXPOSE 4443 8085 8093
ENTRYPOINT ["localgcp"]
CMD ["up", "--no-docker"]
