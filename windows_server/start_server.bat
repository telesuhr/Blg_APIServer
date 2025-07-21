@echo off
REM Bloomberg API Server Startup Script

echo ========================================
echo Bloomberg API Bridge Server
echo ========================================
echo.

REM Check if Bloomberg Terminal is running
tasklist /FI "IMAGENAME eq bbcomm.exe" 2>NUL | find /I /N "bbcomm.exe">NUL
if "%ERRORLEVEL%"=="1" (
    echo WARNING: Bloomberg Terminal ^(bbcomm.exe^) is not running!
    echo The server will start in MOCK MODE.
    echo.
    echo To use real Bloomberg data:
    echo 1. Start Bloomberg Terminal
    echo 2. Log in to Bloomberg
    echo 3. Restart this server
    echo.
    pause
)

REM Check if Python is installed
python --version >NUL 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8 or higher
    pause
    exit /b 1
)

REM Install requirements if needed
if not exist ".deps_installed" (
    echo Installing dependencies...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo ERROR: Failed to install dependencies
        pause
        exit /b 1
    )
    echo. > .deps_installed
)

REM Get local IP address
echo.
echo Server will be available at:
echo   http://localhost:8080

REM Show IPv4 addresses
echo.
echo Your network IP addresses:
ipconfig | findstr /R /C:"IPv4.*[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*"
echo.

REM Start the server
echo Starting Bloomberg API Bridge Server...
echo Press Ctrl+C to stop the server
echo.
python bloomberg_api_server.py

pause