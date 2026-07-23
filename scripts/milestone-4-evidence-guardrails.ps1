Set-StrictMode -Version Latest

function Get-Milestone4NormalizedPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    $fullPath = [System.IO.Path]::GetFullPath($Path)
    $rootPath = [System.IO.Path]::GetPathRoot($fullPath)
    if ($fullPath.Equals($rootPath, [StringComparison]::OrdinalIgnoreCase)) {
        return $rootPath
    }
    return $fullPath.TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
}

function Test-Milestone4SameOrDescendantPath {
    param(
        [Parameter(Mandatory = $true)][string]$Candidate,
        [Parameter(Mandatory = $true)][string]$Parent,
        [Parameter(Mandatory = $true)][StringComparison]$Comparison
    )

    if ($Candidate.Equals($Parent, $Comparison)) { return $true }
    $prefix = $Parent
    if (-not $prefix.EndsWith([string][System.IO.Path]::DirectorySeparatorChar, $Comparison) -and
        -not $prefix.EndsWith([string][System.IO.Path]::AltDirectorySeparatorChar, $Comparison)) {
        $prefix += [System.IO.Path]::DirectorySeparatorChar
    }
    return $Candidate.StartsWith($prefix, $Comparison)
}

function Test-Milestone4ReparsePoint {
    param([Parameter(Mandatory = $true)][System.IO.FileSystemInfo]$Item)

    if (($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        return $true
    }
    $linkType = $Item.PSObject.Properties['LinkType']
    return $null -ne $linkType -and -not [string]::IsNullOrWhiteSpace([string]$linkType.Value)
}

function Assert-Milestone4PathHasNoReparsePoints {
    param([Parameter(Mandatory = $true)][string]$Path)

    $cursor = $Path
    while (-not [string]::IsNullOrWhiteSpace($cursor)) {
        if (Test-Path -LiteralPath $cursor) {
            $item = Get-Item -Force -LiteralPath $cursor
            if (Test-Milestone4ReparsePoint -Item $item) {
                throw 'EvidenceDirectory must not use a reparse point or symbolic-link alias'
            }
        }
        $parent = [System.IO.Directory]::GetParent($cursor)
        if ($null -eq $parent) { break }
        $cursor = $parent.FullName
    }
}

function New-Milestone4EvidenceDirectory {
    param(
        [Parameter(Mandatory = $true)][string]$EvidenceDirectory,
        [Parameter(Mandatory = $true)][string]$RepositoryPath
    )

    $normalizedEvidence = Get-Milestone4NormalizedPath -Path $EvidenceDirectory
    $normalizedRepository = Get-Milestone4NormalizedPath -Path $RepositoryPath
    $isWindowsHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [System.Runtime.InteropServices.OSPlatform]::Windows
    )
    $comparison = if ($isWindowsHost) {
        [StringComparison]::OrdinalIgnoreCase
    } else {
        [StringComparison]::Ordinal
    }
    Assert-Milestone4PathHasNoReparsePoints -Path $normalizedRepository
    if ((Test-Milestone4SameOrDescendantPath -Candidate $normalizedEvidence `
            -Parent $normalizedRepository -Comparison $comparison) -or
        (Test-Milestone4SameOrDescendantPath -Candidate $normalizedRepository `
            -Parent $normalizedEvidence -Comparison $comparison)) {
        throw 'EvidenceDirectory must not overlap the repository or any repository ancestor'
    }
    if (Test-Path -LiteralPath $normalizedEvidence) {
        throw 'EvidenceDirectory must not already exist; this run must create it'
    }

    Assert-Milestone4PathHasNoReparsePoints -Path $normalizedEvidence
    New-Item -ItemType Directory -Path $normalizedEvidence -ErrorAction Stop | Out-Null
    Assert-Milestone4PathHasNoReparsePoints -Path $normalizedEvidence
    if (@(Get-ChildItem -Force -LiteralPath $normalizedEvidence).Count -ne 0) {
        throw 'EvidenceDirectory was not empty immediately after creation'
    }
    return $normalizedEvidence
}

function Assert-Milestone4ComposeProjectUnused {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectName,
        [Parameter(Mandatory = $true)][scriptblock]$DockerInvoker
    )

    if ($ProjectName -notmatch '^[a-z0-9][a-z0-9_-]{2,62}$') {
        throw 'ProjectName must be a bounded lowercase Compose project name'
    }
    $labelFilter = "label=com.docker.compose.project=$ProjectName"
    $queries = @(
        [pscustomobject]@{ Kind = 'container'; Arguments = @('ps', '-a', '-q', '--filter', $labelFilter) },
        [pscustomobject]@{ Kind = 'volume'; Arguments = @('volume', 'ls', '-q', '--filter', $labelFilter) },
        [pscustomobject]@{ Kind = 'network'; Arguments = @('network', 'ls', '-q', '--filter', $labelFilter) }
    )
    foreach ($query in $queries) {
        $result = & $DockerInvoker -DockerArguments ([string[]]$query.Arguments)
        if ($null -eq $result -or $null -eq $result.PSObject.Properties['ExitCode'] -or
            [int]$result.ExitCode -ne 0) {
            throw "could not verify existing Compose $($query.Kind) labels"
        }
        $matches = @($result.Output | Where-Object {
            -not [string]::IsNullOrWhiteSpace([string]$_)
        })
        if ($matches.Count -ne 0) {
            throw 'ProjectName already labels Docker resources; refusing to reuse or tear down that Compose project'
        }
    }
}
