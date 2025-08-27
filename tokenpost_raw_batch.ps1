# ================================
# CONFIG
# ================================
$mongoConn = "mongodb://127.0.0.1:27017/local"
$collection = "pivot.log"
$days_before = 3

# ================================
# 1. 최신 pivot_date 읽기
# ================================
$latest_pivot = mongosh $mongoConn --quiet --eval `
"var d = db.getCollection('$collection')
           .find({})
           .sort({ pivot_date: 1 })
           .limit(1)
           .toArray();
 d.length > 0 ? d[0].pivot_date : ''"

# ================================
# 2. pivot_date 계산
# ================================
if ($latest_pivot -eq "") {
    Write-Host "No pivot.log found. Using default pivot_date = today - 7 days."
    $pivot_date = (Get-Date).AddDays(-7).ToString("yyyyMMdd")
}
else {
    # latest_pivot is string YYYYMMDD
    $dt = [DateTime]::ParseExact($latest_pivot, "yyyyMMdd", $null)

    # compute next pivot_date = previous pivot_date − days_before − 1
    $pivot_date = $dt.AddDays(-$days_before - 1).ToString("yyyyMMdd")
}

Write-Host "Next batch pivot_date:" $pivot_date

# ================================
# 3. FastAPI Batch 실행
# ================================
$url = "http://localhost:8000/api/v1/test/batch?pivot_date=$pivot_date&days_before=$days_before"

Write-Host "Calling API:" $url
$result = Invoke-RestMethod -Method POST -Uri $url -Headers @{
    "Content-Type" = "application/json"
}

Write-Host "Batch result:" $result.message
Write-Host "pivot_date saved to MongoDB: $pivot_date"