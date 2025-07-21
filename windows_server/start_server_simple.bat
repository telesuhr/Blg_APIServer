@echo off
echo ========================================
echo Bloomberg API Bridge Server
echo ========================================
echo.

REM Start the server
echo Starting server at http://localhost:8080
echo.
echo To connect from Macbook, use your Windows PC IP address
echo Run 'ipconfig' in another window to find your IP address
echo.
echo Press Ctrl+C to stop the server
echo.

python bloomberg_api_server.py

pause