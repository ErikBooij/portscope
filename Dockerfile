FROM node:24-alpine AS web
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY index.html tsconfig.json vite.config.ts ./
COPY web ./web
RUN npm run build

FROM golang:1.26.6-alpine AS backend
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
COPY internal ./internal
COPY --from=web /src/internal/webui/dist ./internal/webui/dist
ARG VERSION=dev
ARG COMMIT=
ARG BUILD_DATE=
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X github.com/erikbooij/portscope/internal/buildinfo.Version=${VERSION} -X github.com/erikbooij/portscope/internal/buildinfo.Commit=${COMMIT} -X github.com/erikbooij/portscope/internal/buildinfo.Date=${BUILD_DATE}" -o /portscope .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 portscope
USER portscope
WORKDIR /app
COPY --from=backend /portscope /usr/local/bin/portscope
VOLUME ["/app/.portscope"]
EXPOSE 8090
ENTRYPOINT ["portscope"]
CMD ["--addr", "0.0.0.0:8090"]
