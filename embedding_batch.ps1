# ================================
# CONFIG
# ================================
$mongoConn = "mongodb://127.0.0.1:27017/local"
$collection = "pivot.log"

# ================================
# 1. 최신 pivot_date 읽기 (batch_name = 'embedding')
# ================================
$latest_pivot = mongosh $mongoConn --quiet --eval `
"var d = db.getCollection('$collection')
           .find({ batch_name: 'embedding' })
           .sort({ pivot_date: 1 })
           .limit(1)
           .toArray();
 d.length > 0 ? d[0].pivot_date : ''"

# ================================
# 2. pivot_date 계산 (latest_pivot - 1일)
# ================================
if ($latest_pivot -eq "") {
    Write-Host "No pivot.log found for batch_name='embedding'. Using default pivot_date = today - 1 day."
    $pivot_date = (Get-Date).AddDays(-1).ToString("yyyyMMdd")
}
else {
    # latest_pivot is string YYYYMMDD
    $dt = [DateTime]::ParseExact($latest_pivot, "yyyyMMdd", $null)

    # compute next pivot_date = previous pivot_date - 1
    $pivot_date = $dt.AddDays(-1).ToString("yyyyMMdd")
}

$query = "BTC"

Write-Host "Next batch pivot_date:" $pivot_date
Write-Host "Query:" $query

# ================================
# 3. FastAPI Batch 실행
# ================================
$url = "http://localhost:8003/api/v1/batch/embedding?query=$query&date=$pivot_date"

Write-Host "Calling API:" $url

try {
    $result = Invoke-RestMethod -Method POST -Uri $url -Headers @{
        "Content-Type" = "application/json"
    }

    Write-Host "Batch result:" ($result | ConvertTo-Json)
    Write-Host "Embedding processed for query: $query, pivot_date: $pivot_date"

    # ================================
    # 4. MongoDB에 pivot_date 업데이트 (API 성공 시에만)
    # ================================
    mongosh $mongoConn --quiet --eval `
    "db.getCollection('$collection').insertOne({
        batch_name: 'embedding',
        query: '$query',
        pivot_date: '$pivot_date',
        created_at: new Date()
    })"

    Write-Host "pivot_date saved to MongoDB: $pivot_date"
}
catch {
    Write-Host "API call failed. pivot_date NOT saved to MongoDB."
    Write-Host "Error: $_"
    exit 1
}
