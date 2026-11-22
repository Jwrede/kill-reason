FROM golang:1.24 AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /kill-reason ./cmd/kill-reason

FROM gcr.io/distroless/static:nonroot
COPY --from=build /kill-reason /kill-reason
USER 65532:65532
ENTRYPOINT ["/kill-reason"]
