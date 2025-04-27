FROM golang:1.24-alpine AS builder

RUN apk update && apk upgrade
RUN apk add --no-cache just sqlite
RUN apk add --update gcc musl-dev

WORKDIR /app

COPY . .

RUN just build

FROM alpine:3.21 AS runner

RUN apk update && apk upgrade
RUN apk add --no-cache sqlite
COPY --from=builder /app/emcache /usr/local/bin/emcache

CMD ["emcache"]
