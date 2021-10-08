FROM golang:1.16-alpine
WORKDIR /app
COPY go.mod ./
COPY go.sum ./

RUN go mod download
COPY *.go ./
COPY *.json ./
COPY .env ./

RUN go build -o /docker-gs-ping
EXPOSE 2000
CMD [ "/docker-gs-ping" ]