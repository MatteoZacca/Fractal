# ==========================================
# STAGE 1: The Builder
# ==========================================
FROM golang:1.26-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy the dependency files.
# Docker caches this step, so if you don't change dependencies, 
# it won't re-download them every time you build
COPY go.mod go.sum ./
RUN go mod download

# Copy all Go code into the container
COPY . .

# Compile the Master (NameNode)
# CGO_ENABLED=0 ensures the binary is 100% statically linked (no missing C libraries)
# GOOS=linux forces it to compile for Linux
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/namenode ./cmd/master/main.go

# Compile the Worker (DataNode)
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/datanode ./cmd/worker/main.go

# ==========================================
# STAGE 2: The Runner
# ==========================================
# Start with a completely empty, highly secure Alpine Linux image
FROM alpine:latest

# Add timezone data and basic certificates for secure networking
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy only the compiled binaries  
# The source code is left behind and deleted
COPY --from=builder /app/namenode .
COPY --from=builder /app/datanode .
