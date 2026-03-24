param(
    [switch]$SkipComposeUp,
    [switch]$SkipSampleInsert,
    [string]$TopicName = 'server1.TEST.CUSTOMERS'
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

$connectorName = 'oracle-connector'

function Invoke-ComposeSqlSysdba {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$SqlLines
    )

    $sqlBody = ($SqlLines + 'exit;') -join [Environment]::NewLine
    $command = @"
cat <<'SQL' | sqlplus -s / as sysdba
$sqlBody
SQL
"@

    return podman compose exec oracle bash -lc $command
}

function Wait-Until {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$Condition,
        [Parameter(Mandatory = $true)]
        [string]$Description,
        [int]$TimeoutSeconds = 240,
        [int]$DelaySeconds = 5
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (& $Condition) {
            Write-Host "OK: $Description"
            return
        }
        Start-Sleep -Seconds $DelaySeconds
    }

    throw "Timeout waiting for: $Description"
}

function Get-TopicOffset {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    try {
        $offsets = (& podman compose exec kafka /kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic $Name 2>$null | Out-String).Trim()
    }
    catch {
        return 0
    }

    if (-not $offsets) {
        return 0
    }

    $line = $offsets | Select-Object -First 1
    if ($line -match '^[^:]+:\d+:(\d+)$') {
        return [int64]$Matches[1]
    }

    return 0
}

if (-not $SkipComposeUp) {
    Write-Host 'Starting compose services...'
    podman compose up -d
}

Write-Host 'Waiting for Oracle to accept SYSDBA connections...'
Wait-Until -Description 'Oracle readiness' -Condition {
    try {
        $result = Invoke-ComposeSqlSysdba -SqlLines @(
            'set heading off feedback off verify off echo off pages 0',
            "select 'READY' from dual;"
        )
        return (($result -join "`n") -match 'READY')
    }
    catch {
        return $false
    }
}

Write-Host 'Waiting for Kafka Connect REST API...'
Wait-Until -Description 'Kafka Connect readiness' -Condition {
    try {
        $null = Invoke-RestMethod -Method Get -Uri 'http://localhost:8083/connector-plugins'
        return $true
    }
    catch {
        return $false
    }
}

Write-Host 'Checking Oracle ARCHIVELOG mode...'
$archiveLogStatus = (Invoke-ComposeSqlSysdba -SqlLines @(
    'archive log list;'
) | Out-String)

$isArchiveLog = $archiveLogStatus -match '(?im)^\s*Database log mode\s+Archive Mode\s*$'

if (-not $isArchiveLog) {
    Write-Host 'Enabling ARCHIVELOG mode...'
    Invoke-ComposeSqlSysdba -SqlLines @(
        'shutdown immediate;',
        'startup mount;',
        'alter database archivelog;',
        'alter database open;',
        'archive log list;'
    ) | Out-Host
}
else {
    Write-Host 'ARCHIVELOG already enabled.'
}

$config = [ordered]@{
    'connector.class' = 'io.debezium.connector.oracle.OracleConnector'
    'database.hostname' = 'oracle'
    'database.port' = '1521'
    'database.user' = 'c##dbzuser'
    'database.password' = 'dbz'
    'database.dbname' = 'FREE'
    'database.pdb.name' = 'FREEPDB1'
    'topic.prefix' = 'server1'
    'schema.history.internal.kafka.bootstrap.servers' = 'kafka:9092'
    'schema.history.internal.kafka.topic' = 'schema-changes.inventory'
    'database.connection.adapter' = 'logminer'
}

Write-Host 'Applying connector configuration...'
Invoke-RestMethod -Method Put -Uri "http://localhost:8083/connectors/$connectorName/config" -ContentType 'application/json' -Body ($config | ConvertTo-Json -Depth 6) | Out-Null

# Force a task refresh so stale failures are not kept after DB mode changes.
Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors/$connectorName/tasks/0/restart" -ContentType 'application/json' | Out-Null

Write-Host 'Waiting for connector task to reach RUNNING...'
Wait-Until -Description 'Debezium task running' -Condition {
    try {
        $status = Invoke-RestMethod -Method Get -Uri "http://localhost:8083/connectors/$connectorName/status"
        return ($status.connector.state -eq 'RUNNING' -and $status.tasks.Count -gt 0 -and $status.tasks[0].state -eq 'RUNNING')
    }
    catch {
        return $false
    }
}

$beforeOffset = Get-TopicOffset -Name $TopicName
Write-Host "Current topic offset for ${TopicName}: $beforeOffset"

if (-not $SkipSampleInsert) {
    Write-Host 'Generating sample CDC events via inserir.py...'
    python inserir.py | Out-Host

    if ($LASTEXITCODE -ne 0) {
        throw "inserir.py failed with exit code $LASTEXITCODE; aborting bootstrap before offset wait."
    }

    # Write-Host 'Waiting for topic offset to advance...'
    # Wait-Until -Description 'CDC events available in Kafka' -Condition {
    #     (Get-TopicOffset -Name $TopicName) -gt $beforeOffset
    # } -TimeoutSeconds 180 -DelaySeconds 3
}

$afterOffset = Get-TopicOffset -Name $TopicName
Write-Host "Final topic offset for ${TopicName}: $afterOffset"
Write-Host "Bootstrap complete. Use KAFKA_BOOTSTRAP_SERVERS=localhost:29092 when consuming from the host if needed."