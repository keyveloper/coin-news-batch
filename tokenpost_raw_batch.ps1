# ================================
# CONFIG
# ================================
$mongoConn = "mongodb://127.0.0.1:27017/local"
$collection = "pivot.log"
$days_before = 1

# ================================
# 1. 최신 pivot_date 읽기
# ================================
$latest_pivot = mongosh $mongoConn --quiet --eval `
"var d = db.getCollection('$collection')
           .find({})
           .sort({created_at:-1})
           .limit(1)
           .toArray();
 d.length > 0 ? d[0].pivot_date : ''"

Write-Host "Latest pivot_date from DB:" $latest_pivot

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
$url = "http://localhost:8000/test/batch?pivot_date=$pivot_date&days_before=$days_before"

Write-Host "Calling API:" $url
$result = Invoke-RestMethod -Method POST -Uri $url -Headers @{
    "Content-Type" = "application/json"
}

Write-Host "Batch result:" $result.message

# ================================
# 4. 새로운 pivot_date 저장
# ================================
mongosh $mongoConn --quiet --eval `
"db.getCollection('$collection').insertOne({
    batch_name: 'tokenpost',
    pivot_date: '$pivot_date',
    created_at: new Date()
});"

Write-Host "pivot_date saved to MongoDB: $pivot_date"