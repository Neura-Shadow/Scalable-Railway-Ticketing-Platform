[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'milestone-4-evidence-guardrails.ps1')

function Assert-Throws {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Action,
        [Parameter(Mandatory = $true)][string]$Label
    )
    try {
        & $Action
    } catch {
        return
    }
    throw "expected guardrail rejection: $Label"
}

$sandbox = Join-Path ([System.IO.Path]::GetTempPath()) `
    "railway-m4-guardrail-test-$([guid]::NewGuid().ToString('N'))"
$sandbox = [System.IO.Path]::GetFullPath($sandbox)
$tempRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
if (-not $sandbox.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'guardrail test sandbox escaped the temporary directory'
}

New-Item -ItemType Directory -Path $sandbox | Out-Null
$aliasPath = Join-Path $sandbox 'alias'
try {
    $repository = Join-Path $sandbox 'repository'
    New-Item -ItemType Directory -Path $repository | Out-Null

    $freshEvidence = Join-Path $sandbox 'fresh-evidence'
    $created = New-Milestone4EvidenceDirectory `
        -EvidenceDirectory $freshEvidence -RepositoryPath $repository
    if ($created -ne [System.IO.Path]::GetFullPath($freshEvidence) -or
        -not (Test-Path -LiteralPath $created) -or
        @(Get-ChildItem -Force -LiteralPath $created).Count -ne 0) {
        throw 'fresh evidence directory was not created empty'
    }

    $existingEvidence = Join-Path $sandbox 'existing-evidence'
    New-Item -ItemType Directory -Path $existingEvidence | Out-Null
    Assert-Throws -Label 'pre-existing target' -Action {
        New-Milestone4EvidenceDirectory `
            -EvidenceDirectory $existingEvidence -RepositoryPath $repository | Out-Null
    }
    Assert-Throws -Label 'repository descendant' -Action {
        New-Milestone4EvidenceDirectory `
            -EvidenceDirectory (Join-Path $repository 'evidence') -RepositoryPath $repository | Out-Null
    }
    Assert-Throws -Label 'repository ancestor' -Action {
        New-Milestone4EvidenceDirectory `
            -EvidenceDirectory $sandbox -RepositoryPath $repository | Out-Null
    }

    $aliasTarget = Join-Path $sandbox 'alias-target'
    New-Item -ItemType Directory -Path $aliasTarget | Out-Null
    $isWindowsHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [System.Runtime.InteropServices.OSPlatform]::Windows
    )
    $itemType = if ($isWindowsHost) { 'Junction' } else { 'SymbolicLink' }
    New-Item -ItemType $itemType -Path $aliasPath -Target $aliasTarget | Out-Null
    Assert-Throws -Label 'reparse-point alias' -Action {
        New-Milestone4EvidenceDirectory `
            -EvidenceDirectory (Join-Path $aliasPath 'evidence') -RepositoryPath $repository | Out-Null
    }

    $emptyProbe = {
        param([string[]]$DockerArguments)
        [pscustomobject]@{ ExitCode = 0; Output = @() }
    }
    Assert-Milestone4ComposeProjectUnused -ProjectName 'railway-m4-test' -DockerInvoker $emptyProbe
    Assert-Throws -Label 'invalid Compose project label filter' -Action {
        Assert-Milestone4ComposeProjectUnused `
            -ProjectName 'invalid;project' -DockerInvoker $emptyProbe
    }

    foreach ($busyKind in @('ps', 'volume', 'network')) {
        $busyProbe = {
            param([string[]]$DockerArguments)
            $output = if ($DockerArguments[0] -eq $busyKind) { @('existing-resource') } else { @() }
            [pscustomobject]@{ ExitCode = 0; Output = $output }
        }.GetNewClosure()
        Assert-Throws -Label "existing $busyKind label" -Action {
            Assert-Milestone4ComposeProjectUnused `
                -ProjectName 'railway-m4-test' -DockerInvoker $busyProbe
        }
    }

    $failedProbe = {
        param([string[]]$DockerArguments)
        [pscustomobject]@{ ExitCode = 1; Output = @() }
    }
    Assert-Throws -Label 'metadata query failure' -Action {
        Assert-Milestone4ComposeProjectUnused `
            -ProjectName 'railway-m4-test' -DockerInvoker $failedProbe
    }

    $runner = Get-Content -Raw -LiteralPath `
        (Join-Path $PSScriptRoot 'run-milestone-4-multi-replica-evidence.ps1')
    $collisionCheck = $runner.IndexOf(
        'Assert-Milestone4ComposeProjectUnused -ProjectName $ProjectName',
        [StringComparison]::Ordinal
    )
    $startedFlag = $runner.IndexOf('$started = $true', [StringComparison]::Ordinal)
    $composeUp = $runner.IndexOf("Invoke-Compose -Arguments @('up'", [StringComparison]::Ordinal)
    if ($collisionCheck -lt 0 -or $collisionCheck -gt $startedFlag -or
        $collisionCheck -gt $composeUp) {
        throw 'runner does not reject Compose project collisions before startup and teardown ownership'
    }

    Write-Output 'Milestone 4 evidence guardrail regression tests passed'
} finally {
    if (Test-Path -LiteralPath $aliasPath) {
        Remove-Item -Force -LiteralPath $aliasPath
    }
    if (Test-Path -LiteralPath $sandbox) {
        Remove-Item -Recurse -Force -LiteralPath $sandbox
    }
}
