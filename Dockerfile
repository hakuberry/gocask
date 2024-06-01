FROM golang:1.22

WORKDIR /app

#COPY go.mod ./
#RUN go mod download

COPY . .

CMD ["go", "test", "-v", "data.go", "data_test.go"]
CMD ["go", "test", "-v", "data.go", "gocask.go", "gocask_test.go"]

RUN echo "hello world"

RUN export BUILDKIT_PROGRESS=plain; \
    if [$DEV = "true"]; \
        then echo "huh?"; \
        go test -v ./...; \
    fi