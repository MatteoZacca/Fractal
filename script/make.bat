@echo off
echo Building Fractal CLI...

if not exist "C:\Fractal" mkdir C:\Fractal

go build -o C:\Fractal\fractal.exe ./cmd/client

echo Build complete! fractal.exe has been installed to C:\Fractal