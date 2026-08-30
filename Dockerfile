FROM node:24-alpine AS web
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY index.html tsconfig.json vite.config.ts ./
COPY web ./web
RUN npm run build

FROM golang:1.26-alpine AS backend
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
COPY --from=web /src/internal/webui/dist ./internal/webui/dist
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /portscope ./cmd/portscope

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 portscope
USER portscope
WORKDIR /app
COPY --from=backend /portscope /usr/local/bin/portscope
VOLUME ["/app/data"]
EXPOSE 8090 9081
ENTRYPOINT ["portscope", "--addr", "0.0.0.0:8090", "--data", "/app/data"]
