# power

let
  // 파일 읽기 (UTF-8 우선)
  SourceText =
    let
      utf8 = try Text.FromBinary([Content], 65001)
    in
      if utf8[HasError] then Text.FromBinary([Content]) else utf8[Value],

  // 개행 정규화 및 라인 분리
  txt   = Text.Replace(Text.Replace(SourceText, "#(cr,lf)", "#(lf)"), "#(cr)", "#(lf)"),
  lines = Text.Split(txt, "#(lf)"),
  idx   = List.Positions(lines),

  // ──────────────────────────────────────────────────────────────
  // ① Dac 클래스명: 전역 static 필드에서 inline new로 초기화되는 라인 탐색
  //    예) public static OrderDac _dacUnit = new OrderDac();
  dacCandidates =
    List.Select(
      idx,
      (i) =>
        let l = lines{i} in
          Text.Contains(Text.Lower(l), " static ")
          or Text.StartsWith(Text.Lower(Text.TrimStart(l)), "public static")
          or Text.StartsWith(Text.Lower(Text.TrimStart(l)), "private static")
          or Text.StartsWith(Text.Lower(Text.TrimStart(l)), "protected static")
          or Text.StartsWith(Text.Lower(Text.TrimStart(l)), "internal static")
    ),
  dacLineIndex =
    let
      filtered = List.Select(
                   dacCandidates,
                   (i) =>
                     let l = lines{i} in
                       Text.Contains(l, "=") and Text.Contains(Text.Lower(l), "new ") and Text.Contains(l, "Dac")
                 )
    in if List.Count(filtered) > 0 then filtered{0} else null,

  dacLine = if dacLineIndex <> null then lines{dacLineIndex} else "",

  // new 뒤에서 타입 추출 (최우선)
  dacFromNew =
    if dacLine <> "" and Text.Contains(dacLine, "new ")
    then Text.Trim(Text.BeforeDelimiter(Text.AfterDelimiter(dacLine, "new "), "("))
    else null,

  // 좌변 타입 토큰 보조 추출 (public/private/protected/internal/readonly 조합 대응)
  dacLeftType =
    if dacLine <> "" then
      let
        low = Text.Lower(dacLine),
        afterStatic =
          if Text.Contains(low, "public static") then Text.Trim(Text.AfterDelimiter(dacLine, "public static"))
          else if Text.Contains(low, "private static") then Text.Trim(Text.AfterDelimiter(dacLine, "private static"))
          else if Text.Contains(low, "protected static") then Text.Trim(Text.AfterDelimiter(dacLine, "protected static"))
          else if Text.Contains(low, "internal static") then Text.Trim(Text.AfterDelimiter(dacLine, "internal static"))
          else if Text.Contains(low, "static") then Text.Trim(Text.AfterDelimiter(dacLine, "static"))
          else dacLine,
        afterReadonly =
          if Text.StartsWith(Text.Lower(afterStatic), "readonly")
          then Text.Trim(Text.AfterDelimiter(afterStatic, "readonly"))
          else afterStatic,
        leftPart = Text.BeforeDelimiter(afterStatic, "=", 0),
        typeToken =
          let
            tokens = List.Select(Text.SplitAny(leftPart, " \t"), each _ <> "")
          in if List.Count(tokens) > 0 then tokens{0} else null
      in Text.Trim(typeToken)
    else null,

  DacClassName =
    let primary = if dacFromNew <> null and dacFromNew <> "" then Text.Replace(dacFromNew, ";", "") else null
    in if primary <> null and primary <> "" then primary else dacLeftType,

  // ──────────────────────────────────────────────────────────────
  // ② public static 메서드 한 줄 시그니처 탐지
  methodIdxList =
    List.Select(
      idx,
      (i) =>
        let l = lines{i} in
          Text.Contains(Text.Lower(l), "public static")
          and Text.Contains(l, "(") and Text.Contains(l, ")")
          and not Text.Contains(Text.Lower(l), "class ")
          and not Text.EndsWith(Text.Trim(l), ";") // 대리자/프로토타입 배제
    ),

  // ③ 각 메서드별 정보 추출
  rows =
    List.Transform(
      methodIdxList,
      (i) =>
        let
          line      = lines{i},
          head      = Text.BeforeDelimiter(line, "("),
          headTrim  = Text.Trim(head),
          // Occurrence.Last 대신 토큰 분할로 마지막 토큰(메서드명) 추출
          tokens    = List.Select(Text.SplitAny(headTrim, " \t"), each _ <> ""),
          mName     = if List.Count(tokens) > 0 then List.Last(tokens) else null,

          // 파라미터: 콤마 기준 줄바꿈 (1개 이상 모두 처리)
          paramRaw   = Text.BetweenDelimiters(line, "(", ")", 0, 0),
          paramItems = List.Select(List.Transform(Text.Split(paramRaw, ","), each Text.Trim(_)), each _ <> ""),
          paramOut   = if List.Count(paramItems) = 0 then null else Text.Combine(paramItems, "#(lf)"),

          // <summary> ~ </summary> 를 메서드 위쪽에서 탐색
          prevIdxRev  = List.Reverse(List.FirstN(idx, i)),
          sumStartRel = List.PositionOf(List.Transform(prevIdxRev, (k) => Text.Contains(lines{k}, "<summary>")), true),
          sumStart    = if sumStartRel >= 0 then prevIdxRev{sumStartRel} else null,
          sumEndRel   = if sumStart <> null
                        then List.PositionOf(List.Transform(List.Skip(lines, sumStart), (t) => Text.Contains(t, "</summary>")), true)
                        else -1,
          sumEnd      = if sumEndRel >= 0 then sumStart + sumEndRel else null,
          summaryText =
            if sumStart <> null and sumEnd <> null then
              let
                seg    = List.Range(lines, sumStart, sumEnd - sumStart + 1),
                joined = Text.Combine(seg, " "),
                inner  = if Text.Contains(joined, "<summary>") and Text.Contains(joined, "</summary>")
                         then Text.BetweenDelimiters(joined, "<summary>", "</summary>", 0, 0)
                         else null
              in if inner <> null then Text.Trim(inner) else null
            else null
        in
          [
            DacClass   = DacClassName,
            Method     = mName,
            Summary    = summaryText,
            Parameters = paramOut
          ]
    ),

  // ④ 결과 테이블
  Result = Table.FromRecords(
             rows,
             type table [DacClass = nullable text, Method = nullable text, Summary = nullable text, Parameters = nullable text]
           )
in
  Result


## Power Shell
# Add-LocalPrefix.ps1
[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
  [string]$Path
)

function Select-Folder {
  param(
    [string]$Title = 'Select target folder',
    [string]$InitialDirectory = (Get-Location).Path
  )
  try {
    Add-Type -AssemblyName System.Windows.Forms -ErrorAction Stop
    $dlg = New-Object System.Windows.Forms.FolderBrowserDialog
    $dlg.Description = $Title
    $dlg.SelectedPath = $InitialDirectory
    $dlg.ShowNewFolderButton = $true
    $result = $dlg.ShowDialog()
    if ($result -eq [System.Windows.Forms.DialogResult]::OK -and
        -not [string]::IsNullOrWhiteSpace($dlg.SelectedPath)) {
      return $dlg.SelectedPath
    }
    return $null
  } catch {
    Write-Warning 'Folder dialog failed. Falling back to current folder.'
    return $null
  }
}

# Input handling: if no -Path, open folder picker; if canceled -> current folder
if (-not $PSBoundParameters.ContainsKey('Path') -or [string]::IsNullOrWhiteSpace($Path)) {
  $picked = Select-Folder -Title 'Select target folder' -InitialDirectory (Get-Location).Path
  $Path = if ($picked) { $picked } else { (Get-Location).Path }
}

# Validate path
$resolved = Resolve-Path -LiteralPath $Path -ErrorAction SilentlyContinue
if (-not $resolved) {
  Write-Error ('Path not found: {0}' -f $Path)
  return
}
$root = $resolved.Path

# Word/Excel/PowerPoint only
$officeExt = @(
  # Word
  '.doc', '.docx', '.docm', '.dot', '.dotx', '.dotm',
  # Excel
  '.xls', '.xlsx', '.xlsm', '.xlsb', '.xlt', '.xltx', '.xltm',
  # PowerPoint
  '.ppt', '.pptx', '.pptm', '.pot', '.potx', '.potm', '.pps', '.ppsx', '.ppsm'
)

function Get-UniqueFileName {
  param(
    [Parameter(Mandatory=$true)][string]$Directory,
    [Parameter(Mandatory=$true)][string]$BaseWithPrefix,  # '(Local) ' + BaseName
    [Parameter(Mandatory=$true)][string]$Extension
  )
  $candidate = Join-Path $Directory ($BaseWithPrefix + $Extension)
  if (-not (Test-Path -LiteralPath $candidate)) {
    return [System.IO.Path]::GetFileName($candidate)
  }
  $i = 1
  while ($true) {
    $trialName = '{0} ({1}){2}' -f $BaseWithPrefix, $i, $Extension
    $trialPath = Join-Path $Directory $trialName
    if (-not (Test-Path -LiteralPath $trialPath)) {
      return $trialName
    }
    $i++
  }
}

# Enumerate files: exclude Hidden/System attributes
Get-ChildItem -LiteralPath $root -Recurse -File -ErrorAction SilentlyContinue |
  Where-Object {
    (($_.Attributes -band [IO.FileAttributes]::Hidden) -eq 0) -and
    (($_.Attributes -band [IO.FileAttributes]::System) -eq 0)
  } |
  ForEach-Object {
    $ext = ''
    if ($_.Extension) { $ext = $_.Extension.ToLowerInvariant() }
    if ($officeExt -notcontains $ext) { return }

    $base = $_.BaseName

    # Skip if starts with (Work), or already starts with (Local)
    if ($base -match '^\(Work\)\s*' -or $base -match '^\(Local\)\s*') { return }

    $prefixedBase = '(Local) ' + $base
    $newName = Get-UniqueFileName -Directory $_.DirectoryName -BaseWithPrefix $prefixedBase -Extension $ext

    if ($PSCmdlet.ShouldProcess($_.FullName, 'Rename to ' + $newName)) {
      try {
        Rename-Item -LiteralPath $_.FullName -NewName $newName
      } catch {
        Write-Warning ('Rename failed: {0} -> {1} | {2}' -f $_.FullName, $newName, $_.Exception.Message)
      }
    }
  }

Write-Host ('Done: Applied ''(Local)'' prefix to Word/Excel/PowerPoint files (Hidden/System excluded) under: {0}' -f $root)
