# Run in your solution root
# Outputs: .\bpmn-nodes.csv, .\bpmn-edges.csv

$controllers = @()
$actions = @()
$edges = @{}

# Regex
$rxController = [regex]'class\s+(\w+)Controller\s*:\s*Controller'
$rxAction     = [regex]'public\s+(?:async\s+)?(?:ActionResult|Task\<ActionResult\>|JsonResult|FileResult|HttpStatusCodeResult)\s+(\w+)\s*\('
$rxRedirect   = [regex]'RedirectToAction\s*\(\s*"(?<act>\w+)"\s*(?:,\s*"(?<ctl>\w+)")?'
$rxView       = [regex]'return\s+View\s*\(\s*"?(?<view>\w+)"?'
$rxAuth       = [regex]'\[Authorize(?:\s*\(\s*Roles\s*=\s*"(?<roles>[^"]+)"\s*\))?\s*\]'
$rxHttpPost   = [regex]'\[HttpPost\]'
$rxHttpGet    = [regex]'\[HttpGet\]'

Get-ChildItem -Recurse -Filter *.cs | ForEach-Object {
  $path = $_.FullName
  $text = Get-Content $path -Raw

  $mCtl = $rxController.Matches($text)
  foreach ($m in $mCtl) {
    $ctl = $m.Groups[1].Value
    $controllers += [pscustomobject]@{ Controller = $ctl; File = $path }

    # action 영역 스캔
    $lines = $text -split "`n"
    for ($i=0; $i -lt $lines.Count; $i++) {
      if ($rxAction.IsMatch($lines[$i])) {
        $act = $rxAction.Match($lines[$i]).Groups[1].Value

        # 근처의 HttpAttr/AuthAttr 추출(상단 5줄 범위)
        $roles = ''
        $verb  = 'ANY'
        for ($k=[Math]::Max(0,$i-5); $k -le $i; $k++) {
          if ($rxAuth.IsMatch($lines[$k])) { $roles = $rxAuth.Match($lines[$k]).Groups['roles'].Value }
          if ($rxHttpPost.IsMatch($lines[$k])) { $verb='POST' }
          elseif ($rxHttpGet.IsMatch($lines[$k])) { $verb='GET' }
        }

        $id = "$ctl.$act"
        $actions += [pscustomobject]@{
          Id = $id; Controller=$ctl; Action=$act; Verb=$verb; Roles=$roles; File=$path; Line=$i+1
        }

        # 현재 액션 블록 내 Redirect 수집(대략적인 60줄 창)
        $block = ($lines[ [Math]::Max(0,$i) .. [Math]::Min($lines.Count-1,$i+60) ]) -join "`n"
        foreach ($mm in $rxRedirect.Matches($block)) {
          $toAct = $mm.Groups['act'].Value
          $toCtl = if ($mm.Groups['ctl'].Success) { $mm.Groups['ctl'].Value } else { $ctl }
          $toId  = "$toCtl.$toAct"
          $key   = "$id=>$toId"
          if (-not $edges.ContainsKey($key)) { $edges[$key] = [pscustomobject]@{ From=$id; To=$toId; Label='Redirect' } }
        }

        # View 반환은 종착점 표시(선택)
        foreach ($mv in $rxView.Matches($block)) {
          $view = $mv.Groups['view'].Value
          $toId = "$ctl.$act.View.$view"
          $key  = "$id=>$toId"
          if (-not $edges.ContainsKey($key)) { $edges[$key] = [pscustomobject]@{ From=$id; To=$toId; Label='Return View' } }
        }
      }
    }
  }
}

# 노드 CSV
$nodeRows = @()
$actionSet = $actions | Group-Object Id | ForEach-Object { $_.Group[0] }
foreach ($a in $actionSet) {
  $style = 'shape=mxgraph.bpmn.task;whiteSpace=wrap;html=1;'
  if ($a.Verb -eq 'GET')  { $style = 'shape=mxgraph.bpmn.task;whiteSpace=wrap;html=1;' }
  if ($a.Verb -eq 'POST') { $style = 'shape=mxgraph.bpmn.task;whiteSpace=wrap;html=1;' }
  if ($a.Roles -ne '')    { $style += 'strokeColor=#ff9900;' } # 권한있으면 색 표시

  $nodeRows += [pscustomobject]@{
    id    = $a.Id
    label = "$($a.Controller).$($a.Action) [$($a.Verb)]"
    style = $style
  }
}
# View 종착점도 노드로
$viewNodes = $edges.Values | Where-Object { $_.Label -eq 'Return View' } | Select-Object -ExpandProperty To -Unique
foreach ($vn in $viewNodes) {
  $nodeRows += [pscustomobject]@{
    id=$vn; label=$vn.Split('.')[-1]; style='shape=mxgraph.bpmn.none_end_event;verticalLabelPosition=bottom;verticalAlign=top;'
  }
}

$nodeRows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path .\bpmn-nodes.csv

# 엣지 CSV
$edges.Values | Export-Csv -NoTypeInformation -Encoding UTF8 -Path .\bpmn-edges.csv
Write-Host "Done: bpmn-nodes.csv, bpmn-edges.csv"
