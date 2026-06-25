all: build

build: 
	@echo "Building Fractal CLI for Mac/Linux..."
	@go build -o fractal ./cmd/client
	@echo "Build complete! Moving to /usr/local/bin..."
	@sudo mv fractal /usr/local/bin/fractal
	@echo "You can now use the 'fractal' command anywhere!"

	