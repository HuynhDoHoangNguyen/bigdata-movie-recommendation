# ==============================================================================
# Script khởi động nhanh Web Demo TV4 - Big Data Movie Recommendation
# ==============================================================================

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "   🎬 KHỞI ĐỘNG BIG DATA MOVIE RECOMMENDATION WEB DEMO" -ForegroundColor Yellow
Write-Host "==========================================================" -ForegroundColor Cyan

# Kiểm tra python
$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    Write-Host "[LỖI] Không tìm thấy Python trong PATH!" -ForegroundColor Red
    exit 1
}

# Kiểm tra flask
Write-Host "[1/3] Đang kiểm tra thư viện Flask..." -ForegroundColor Gray
$flaskCheck = python -c "import flask; print(flask.__version__)" 2>$null
if (-not $flaskCheck) {
    Write-Host "[!] Đang cài đặt flask..." -ForegroundColor Yellow
    python -m pip install -r web/requirements.txt
} else {
    Write-Host "[OK] Flask version $flaskCheck đã sẵn sàng." -ForegroundColor Green
}

# Chạy test tự động trước
Write-Host "[2/3] Đang chạy kiểm thử tự động hệ thống web..." -ForegroundColor Gray
python web/test_web.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "[CẢNH BÁO] Kiểm thử có lỗi phát sinh. Vui lòng kiểm tra lại data." -ForegroundColor Yellow
} else {
    Write-Host "[OK] Toàn bộ 10/10 test cases đã vượt qua thành công!" -ForegroundColor Green
}

# Khởi động Web Server
Write-Host "[3/3] Đang khởi động Web Demo tại cổng 5000..." -ForegroundColor Gray
Write-Host ""
Write-Host "👉 Truy cập Web Demo tại: http://localhost:5000" -ForegroundColor Cyan
Write-Host "👉 Nhấn Ctrl+C để dừng ứng dụng." -ForegroundColor DarkGray
Write-Host ""

python web/app.py
