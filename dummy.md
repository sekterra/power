<# make-dummy.ps1 #>
param(
  [string]$Path,
  [int]$SizeMB = 1024  # 기본 1 GiB
)

if ([string]::IsNullOrWhiteSpace($Path)) {
  $Path = Read-Host "생성할 파일명(경로 포함)을 입력하세요 (예: C:\temp\big_1GiB.bin)"
  if ([string]::IsNullOrWhiteSpace($Path)) {
    Write-Error "파일명이 비어 있습니다. 종료합니다."
    exit 1
  }
}

if ($SizeMB -lt 1) { Write-Error "SizeMB 는 1 이상이어야 합니다."; exit 1 }

$dir = Split-Path -Parent $Path
if ($dir -and -not (Test-Path -LiteralPath $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }

$chunk = New-Object byte[] (1MB)
$fs = $null
try {
  $fs = [System.IO.File]::Open($Path, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
  $fs.SetLength(0)

  for ($i = 0; $i -lt $SizeMB; $i++) {
    $pct = [int](($i / $SizeMB) * 100)
    Write-Progress -Activity "더미 파일 생성 중" -Status "$($i+1)/$SizeMB MiB 작성" -PercentComplete $pct
    $fs.Write($chunk, 0, $chunk.Length)
  }
  Write-Progress -Activity "더미 파일 생성 완료" -Completed

  $bytes = $SizeMB * 1MB
  $sizeStr = "{0:N0}" -f $bytes
  Write-Host "완료: $Path ($sizeStr bytes)" -ForegroundColor Green
}
catch {
  Write-Error "파일 생성 중 오류: $($_.Exception.Message)"
}
finally {
  if ($fs) { $fs.Dispose() }
}
