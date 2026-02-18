FROM golang:1.26-trixie AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=1 go build -o /out/pipeline ./cmd/pipeline

FROM debian:trixie-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg ca-certificates apt-transport-https unixodbc libstdc++6 \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends mssql-tools18 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /out/pipeline /app/pipeline
COPY entrypoint.sh /app/entrypoint.sh
COPY sql /app/sql

RUN chmod +x /app/entrypoint.sh /app/pipeline

ENTRYPOINT ["/app/entrypoint.sh"]
