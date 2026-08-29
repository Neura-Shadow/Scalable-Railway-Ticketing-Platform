function New-M7K6TraversalState {
    param([ValidateRange(1, 4096)][int]$Limit = 4096)

    return [pscustomobject]@{
        Visited = 0
        Limit = $Limit
        Truncated = $false
        Counts = @{}
    }
}

function Use-M7K6TraversalBudget {
    param(
        [Parameter(Mandatory=$true)][object]$State,
        [Parameter(Mandatory=$true)][ValidateSet('object_node','property','check','threshold','array','array_element')][string]$Kind
    )

    if ([bool]$State.Truncated) { return $false }
    if ([int]$State.Visited -ge [int]$State.Limit) {
        $State.Truncated = $true
        return $false
    }
    $State.Visited = [int]$State.Visited + 1
    if (-not $State.Counts.ContainsKey($Kind)) { $State.Counts[$Kind] = 0 }
    $State.Counts[$Kind] = [int]$State.Counts[$Kind] + 1
    return $true
}

function Get-M7K6TraversalProperty {
    param(
        [AllowNull()][object]$Value,
        [Parameter(Mandatory=$true)][string]$Name,
        [Parameter(Mandatory=$true)][object]$TraversalState
    )

    if ($null -eq $Value -or -not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'property')) { return $null }
    return $Value.PSObject.Properties[$Name]
}

function Get-M7K6FailedCheckNames {
    param(
        [Parameter(Mandatory=$true)][object]$Group,
        [AllowNull()][object]$TraversalState = $null
    )

    $names = [System.Collections.Generic.List[string]]::new()
    if ($null -eq $TraversalState) { $TraversalState = New-M7K6TraversalState }
    function Visit-M7K6Group {
        param([object]$Value, [int]$Depth)
        if ($null -eq $Value -or [bool]$TraversalState.Truncated) { return }
        if ($Depth -gt 12) { $TraversalState.Truncated = $true; return }
        if (-not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'object_node')) { return }
        $checksProperty = Get-M7K6TraversalProperty -Value $Value -Name 'checks' -TraversalState $TraversalState
        if ($null -ne $checksProperty -and $null -ne $checksProperty.Value) {
            if (-not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'object_node')) { return }
            foreach ($check in $checksProperty.Value.PSObject.Properties) {
                if ([bool]$TraversalState.Truncated) { break }
                if (-not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'property') -or
                    -not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'check')) { break }
                if (-not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'object_node')) { break }
                $failsProperty = Get-M7K6TraversalProperty -Value $check.Value -Name 'fails' -TraversalState $TraversalState
                if ($null -ne $failsProperty -and [int64]$failsProperty.Value -gt 0 -and $names.Count -lt 20) {
                    $nameProperty = Get-M7K6TraversalProperty -Value $check.Value -Name 'name' -TraversalState $TraversalState
                    $name = if ($null -ne $nameProperty) { [string]$nameProperty.Value } else { [string]$check.Name }
                    $names.Add($name)
                }
            }
        }
        if ([bool]$TraversalState.Truncated) { return }
        $groupsProperty = Get-M7K6TraversalProperty -Value $Value -Name 'groups' -TraversalState $TraversalState
        if ($null -ne $groupsProperty -and $null -ne $groupsProperty.Value) {
            if (-not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'object_node')) { return }
            foreach ($child in $groupsProperty.Value.PSObject.Properties) {
                if ([bool]$TraversalState.Truncated) { break }
                if (-not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'property')) { break }
                Visit-M7K6Group -Value $child.Value -Depth ($Depth + 1)
            }
        }
    }
    Visit-M7K6Group -Value $Group -Depth 0
    return [string[]]$names
}

function Get-M7K6MetricValues {
    param(
        [Parameter(Mandatory=$true)][object]$Summary,
        [Parameter(Mandatory=$true)][string]$MetricName
    )

    $metricsProperty = $Summary.PSObject.Properties['metrics']
    if ($null -eq $metricsProperty -or $null -eq $metricsProperty.Value) { return $null }
    $metricProperty = $metricsProperty.Value.PSObject.Properties[$MetricName]
    if ($null -eq $metricProperty -or $null -eq $metricProperty.Value) { return $null }
    $valuesProperty = $metricProperty.Value.PSObject.Properties['values']
    if ($null -ne $valuesProperty -and $null -ne $valuesProperty.Value) { return $valuesProperty.Value }
    return $metricProperty.Value
}

function Get-M7K6FailedThresholds {
    param(
        [Parameter(Mandatory=$true)][object]$Value,
        [AllowNull()][object]$TraversalState = $null
    )

    $results = [System.Collections.Generic.List[object]]::new()
    if ($null -eq $TraversalState) { $TraversalState = New-M7K6TraversalState }
    function Visit-M7K6ThresholdNode {
        param([object]$Node, [string]$Name, [int]$Depth)
        if ($null -eq $Node -or [bool]$TraversalState.Truncated) { return }
        if ($Depth -gt 12) { $TraversalState.Truncated = $true; return }
        if (-not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'object_node')) { return }
        if ($Node -is [string] -or $Node.GetType().IsPrimitive) { return }
        if ($Node -is [System.Collections.IEnumerable] -and $Node -isnot [System.Management.Automation.PSCustomObject]) {
            if (-not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'array')) { return }
            foreach ($item in $Node) {
                if (-not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'array_element')) { break }
                Visit-M7K6ThresholdNode -Node $item -Name $Name -Depth ($Depth + 1)
                if ([bool]$TraversalState.Truncated) { break }
            }
            return
        }
        $thresholds = Get-M7K6TraversalProperty -Value $Node -Name 'thresholds' -TraversalState $TraversalState
        if ($null -ne $thresholds -and $null -ne $thresholds.Value) {
            $crossed = $false
            if (-not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'object_node')) { return }
            foreach ($threshold in $thresholds.Value.PSObject.Properties) {
                if (-not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'property') -or
                    -not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'threshold')) { break }
                if ([bool]$threshold.Value) { $crossed = $true }
            }
            if ($crossed -and -not [bool]$TraversalState.Truncated -and $results.Count -lt 20) {
                $countProperty = Get-M7K6TraversalProperty -Value $Node -Name 'count' -TraversalState $TraversalState
                $results.Add([pscustomobject]@{
                    Name = $Name
                    ZeroSamples = ($null -ne $countProperty -and [int64]$countProperty.Value -eq 0)
                })
            }
        }
        foreach ($property in $Node.PSObject.Properties) {
            if ($property.Name -eq 'thresholds') { continue }
            if ([bool]$TraversalState.Truncated) { break }
            if (-not (Use-M7K6TraversalBudget -State $TraversalState -Kind 'property')) { break }
            Visit-M7K6ThresholdNode -Node $property.Value -Name $property.Name -Depth ($Depth + 1)
        }
    }
    Visit-M7K6ThresholdNode -Node $Value -Name 'unknown' -Depth 0
    return [object[]]$results
}

function Get-M7K6RuntimeErrorCategory {
    param([string[]]$LogLines = @())

    $tail = [string](@($LogLines | Select-Object -Last 40) -join "`n")
    if ($tail -match '(?i)failed to handle the end-of-test summary|could not save some summary information|summary[^\r\n]{0,120}(permission denied|read-only file system)') {
        return 'summary_write_failure'
    }
    if ($tail -match '(?i)(dial tcp[^\r\n]*lookup|no such host|server misbehaving|temporary failure in name resolution)') {
        return 'docker_network_resolution_failure'
    }
    if ($tail -match '(?i)\b(ReferenceError|TypeError|SyntaxError|GoError)\b|uncaught javascript') {
        return 'javascript_exception'
    }
    if ($tail -match '(?i)(request timeout|context deadline exceeded|connection refused|connection reset)') {
        return 'http_transport_error'
    }
    return 'none'
}

function Protect-M7K6DiagnosticText {
    param(
        [AllowEmptyString()][string]$Text,
        [string[]]$SensitiveValues = @(),
        [int]$MaximumLength = 160
    )

    $value = [string]$Text
    $trimmedValue = $value.TrimStart()
    if ($value -match '(?i)(response[_ ]?body|body\s*[:=])' -or $trimmedValue.StartsWith('{') -or $trimmedValue.StartsWith('[')) {
        return '[omitted unstructured response content]'
    }
    foreach ($secret in @($SensitiveValues | Where-Object { -not [string]::IsNullOrEmpty($_) } | Sort-Object Length -Descending -Unique)) {
        $value = $value.Replace([string]$secret, '[redacted]')
    }
    $value = $value -replace '(?i)\bauthorization\s*[:=]\s*[^\r\n]+', '[redacted-authorization]'
    $value = $value -replace '(?i)\b(?:database[_-]?host|db[_-]?host|host(?:name)?)\s*[:=]\s*[^\s",;]+', '[redacted-host]'
    $value = $value -replace '(?i)\b((?:dial tcp|lookup)\s+)[^\s",;]+', '$1[redacted-host]'
    $value = $value -replace '(?i)((?:passenger(?:[_-]?(?:name|email|phone|id))?)\s*[:=]\s*)[^\s",;]+', '$1[redacted-passenger]'
    $value = $value -replace '(?i)\b(?:postgres(?:ql)?|redis)://[^\s"'',;]+', '[redacted-dsn]'
    $value = $value -replace '(?i)\b[a-z][a-z0-9+.-]*://[^\s/@:]+:[^\s/@]+@[^\s"'',;]+', '[redacted-url]'
    $value = $value -replace '(?i)\bhttps?://[^\s"'',;]+', '[redacted-url]'
    $value = $value -replace '(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b', '[redacted-email]'
    $value = $value -replace '(?<![A-Za-z0-9])\+?\d(?:[\s().-]*\d){7,14}(?![A-Za-z0-9])', '[redacted-phone]'
    $value = $value -replace '(?i)\b(?:rk|sk)_(?:test|live)_[A-Za-z0-9._-]+', '[redacted-api-key]'
    $value = $value -replace '(?i)\bwhsec_[A-Za-z0-9._-]+', '[redacted-webhook-secret]'
    $value = $value -replace '(?i)\bBearer\s+[^\s",;]+', 'Bearer [redacted]'
    $value = $value -replace '(?i)((?:authorization|api[_-]?key|provider[_-]?secret|webhook[_-]?secret|jwt|dsn|password|secret)\s*[:=]\s*)[^\s",;]+', '$1[redacted]'
    $value = $value -replace '(?i)\b[A-Za-z0-9._-]*(?:secret|token|password)[A-Za-z0-9._-]+\b', '[redacted-secret]'
    $value = $value -replace '\beyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b', '[redacted-jwt]'
    $value = $value -replace '(?i)\b(?:pi|pm|seti|res|reservation|ticket|user)_[A-Za-z0-9._-]+', '[redacted-id]'
    $value = $value -replace '(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b', '[redacted-id]'
    $value = $value -replace '[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', ''
    $value = $value.Trim()
    if ($MaximumLength -lt 16) { $MaximumLength = 16 }
    if ($value.Length -gt $MaximumLength) { $value = $value.Substring(0, $MaximumLength) }
    return $value
}

function Get-M7K6SanitizedLogTail {
    param([string[]]$LogLines = @(), [string[]]$SensitiveValues = @())

    $result = [System.Collections.Generic.List[string]]::new()
    foreach ($line in @($LogLines | Select-Object -Last 40)) {
        $safe = Protect-M7K6DiagnosticText -Text ([string]$line) -SensitiveValues $SensitiveValues -MaximumLength 192
        if (-not [string]::IsNullOrWhiteSpace($safe)) { $result.Add($safe) }
    }
    return [string[]]$result
}

function Get-M7K6Diagnostic {
    param(
        [Parameter(Mandatory=$true)][string]$Script,
        [Parameter(Mandatory=$true)][int]$ExitCode,
        [Parameter(Mandatory=$true)][string]$SummaryPath,
        [string[]]$LogLines = @(),
        [string[]]$SensitiveValues = @()
    )

    $summaryPresent = ([System.IO.File]::Exists($SummaryPath) -and [System.IO.FileInfo]::new($SummaryPath).Length -gt 0)
    $summaryValid = $false
    $summaryMalformed = $false
    $checkPasses = 0L
    $checkFailures = 0L
    $iterations = 0L
    $failedChecks = [string[]]@()
    $failedThresholdDetails = [object[]]@()
    $traversalState = New-M7K6TraversalState -Limit 4096
    $metricsProperty = $null
    $checksProperty = $null
    $iterationsProperty = $null
    if ($summaryPresent) {
        try {
            $summary = Get-Content -Raw -LiteralPath $SummaryPath | ConvertFrom-Json
            $summaryValid = $true
            if (Use-M7K6TraversalBudget -State $traversalState -Kind 'object_node') {
                $metricsProperty = Get-M7K6TraversalProperty -Value $summary -Name 'metrics' -TraversalState $traversalState
            }
            if ($null -ne $metricsProperty) {
                if (-not (Use-M7K6TraversalBudget -State $traversalState -Kind 'object_node')) { $metricsProperty = $null }
            }
            if ($null -ne $metricsProperty) {
                $checksProperty = Get-M7K6TraversalProperty -Value $metricsProperty.Value -Name 'checks' -TraversalState $traversalState
            }
            if ($null -ne $checksProperty) {
                if (-not (Use-M7K6TraversalBudget -State $traversalState -Kind 'object_node')) { $checksProperty = $null }
            }
            if ($null -ne $checksProperty) {
                $passesProperty = Get-M7K6TraversalProperty -Value $checksProperty.Value -Name 'passes' -TraversalState $traversalState
                $failsProperty = Get-M7K6TraversalProperty -Value $checksProperty.Value -Name 'fails' -TraversalState $traversalState
                if ($null -ne $passesProperty) { $checkPasses = [int64]$passesProperty.Value }
                if ($null -ne $failsProperty) { $checkFailures = [int64]$failsProperty.Value }
            }
            if ($null -ne $metricsProperty) {
                $iterationsProperty = Get-M7K6TraversalProperty -Value $metricsProperty.Value -Name 'iterations' -TraversalState $traversalState
            }
            if ($null -ne $iterationsProperty) {
                if (-not (Use-M7K6TraversalBudget -State $traversalState -Kind 'object_node')) { $iterationsProperty = $null }
            }
            if ($null -ne $iterationsProperty) {
                $countProperty = Get-M7K6TraversalProperty -Value $iterationsProperty.Value -Name 'count' -TraversalState $traversalState
                if ($null -ne $countProperty) { $iterations = [int64]$countProperty.Value }
            }
            $rootGroupProperty = Get-M7K6TraversalProperty -Value $summary -Name 'root_group' -TraversalState $traversalState
            if ($null -ne $rootGroupProperty) {
                $failedChecks = [string[]]@(Get-M7K6FailedCheckNames -Group $rootGroupProperty.Value -TraversalState $traversalState)
            }
            if (-not [bool]$traversalState.Truncated) {
                $failedThresholdDetails = [object[]]@(Get-M7K6FailedThresholds -Value $summary -TraversalState $traversalState)
            }
        } catch {
            $summaryValid = $false
            $summaryMalformed = $true
        }
    }
    $runtimeError = Get-M7K6RuntimeErrorCategory -LogLines $LogLines
    $classification = if ($summaryMalformed) {
        'evidence_runner_diagnostic_failure'
    } elseif ([bool]$traversalState.Truncated) {
        'evidence_runner_diagnostic_failure'
    } elseif (-not $summaryPresent -and $runtimeError -eq 'summary_write_failure') {
        'k6_summary_write_failure'
    } elseif ($checkFailures -gt 0) {
        'k6_check_failure'
    } elseif (@($failedThresholdDetails | Where-Object { [bool]$_.ZeroSamples }).Count -gt 0) {
        'k6_inherited_threshold_without_samples'
    } elseif ($failedThresholdDetails.Count -gt 0) {
        'k6_threshold_regression'
    } elseif ($runtimeError -eq 'docker_network_resolution_failure') {
        'docker_network_resolution_failure'
    } elseif ($runtimeError -eq 'javascript_exception') {
        'k6_runtime_exception'
    } else {
        'unknown'
    }
    $safeFailedChecks = [string[]]@($failedChecks | Select-Object -First 20 | ForEach-Object {
        Protect-M7K6DiagnosticText -Text ([string]$_) -SensitiveValues $SensitiveValues -MaximumLength 128
    })
    $safeFailedThresholds = [string[]]@($failedThresholdDetails | Select-Object -First 20 | ForEach-Object {
        Protect-M7K6DiagnosticText -Text ([string]$_.Name) -SensitiveValues $SensitiveValues -MaximumLength 128
    })
    $result = [ordered]@{
        script = $Script
        exit_code = $ExitCode
        summary_present = $summaryPresent
        summary_valid = $summaryValid
        diagnostic_truncated = [bool]$traversalState.Truncated
        summary_inspection_complete = ($summaryPresent -and $summaryValid -and -not [bool]$traversalState.Truncated)
        check_passes = $checkPasses
        check_failures = $checkFailures
        failed_checks = $safeFailedChecks
        failed_thresholds = $safeFailedThresholds
        iterations = $iterations
        runtime_error = $runtimeError
        classification = $classification
        log_tail = Get-M7K6SanitizedLogTail -LogLines $LogLines -SensitiveValues $SensitiveValues
    }
    while (($result | ConvertTo-Json -Depth 10 -Compress).Length -gt 4000 -and @($result.log_tail).Count -gt 0) {
        $result.log_tail = [string[]]@($result.log_tail | Select-Object -Last (@($result.log_tail).Count - 1))
    }
    while (($result | ConvertTo-Json -Depth 10 -Compress).Length -gt 4000 -and @($result.failed_thresholds).Count -gt 0) {
        $result.failed_thresholds = [string[]]@($result.failed_thresholds | Select-Object -First (@($result.failed_thresholds).Count - 1))
    }
    while (($result | ConvertTo-Json -Depth 10 -Compress).Length -gt 4000 -and @($result.failed_checks).Count -gt 1) {
        $result.failed_checks = [string[]]@($result.failed_checks | Select-Object -First (@($result.failed_checks).Count - 1))
    }
    return $result
}
