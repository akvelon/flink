# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

param(
    [Parameter(Mandatory = $true)][string] $IcebergRuntimeJar,
    [Parameter(Mandatory = $true)][string] $GcsClasspath,
    [Parameter(Mandatory = $true)][string] $CloudResources,
    [Parameter(Mandatory = $true)][string] $OutputDirectory,
    [string] $Fixtures = 'target/g11/fixtures/fixtures.json',
    [string] $ReadbackManifest,
    [string] $JavaHome = $env:JAVA_HOME
)

$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
Push-Location $repo
try {
    $report = 'flink-formats/flink-parquet/target/surefire-reports/TEST-org.apache.flink.formats.parquet.row.ParquetRowDataWriterTest.xml'
    $xml = [xml](Get-Content -LiteralPath $report -Raw)
    $baseClasspath = ($xml.testsuite.properties.property |
        Where-Object name -eq 'surefire.test.class.path').value
    if (!$baseClasspath) {
        throw 'Run the focused Maven tests first to populate the runtime classpath.'
    }
    if ($baseClasspath -match 'calcite[^;]*(g08|g10)-SNAPSHOT') {
        throw 'The runtime classpath still references a patched Calcite dependency.'
    }
    $cloudEntries = (Get-Content -LiteralPath $GcsClasspath -Raw).Trim() -split [IO.Path]::PathSeparator
    $hadoopEntries = @($cloudEntries | Where-Object { $_ -match 'hadoop-client-(api|runtime)[\\/]3\.3\.6' })
    if ($hadoopEntries.Count -ne 2) {
        throw 'Resolve the manual runtime with both Hadoop 3.3.6 client artifacts.'
    }
    $entries = @((Resolve-Path 'flink-table/flink-table-planner/target/classes').Path)
    $entries += $hadoopEntries
    $entries += $baseClasspath -split [IO.Path]::PathSeparator
    $entries += (Resolve-Path -LiteralPath $IcebergRuntimeJar).Path
    $entries += $cloudEntries
    $entries = @($entries | Where-Object {
        $_ -and $_ -notmatch 'flink-table-planner-loader' -and
        ($_ -notmatch '[\\/]org[\\/]apache[\\/]hadoop[\\/]' -or $_ -in $hadoopEntries)
    } |
        Select-Object -Unique)
    $classpath = ($entries -join [IO.Path]::PathSeparator).Replace('\', '/')
    New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
    $output = (Resolve-Path -LiteralPath $OutputDirectory).Path
    $argumentsFile = Join-Path $output 'probe.args'
    $source = (Join-Path $PSScriptRoot 'G11IcebergProbe.java').Replace('\', '/')
    $fixturePath = (Resolve-Path -LiteralPath $Fixtures).Path.Replace('\', '/')
    $cloudPath = (Resolve-Path -LiteralPath $CloudResources).Path.Replace('\', '/')
    @(
        '-ea'
        '-Xmx2g'
        '--add-opens=java.base/java.lang=ALL-UNNAMED'
        '--add-opens=java.base/java.util=ALL-UNNAMED'
        '--add-opens=java.base/java.io=ALL-UNNAMED'
        '--add-opens=java.base/java.nio=ALL-UNNAMED'
        '-cp'
        ('"' + $classpath + '"')
        ('"' + $source + '"')
        ('"' + $fixturePath + '"')
        ('"' + $cloudPath + '"')
        ('"' + $output.Replace('\', '/') + '"')
    ) | Set-Content -LiteralPath $argumentsFile -Encoding utf8NoBOM
    if ($ReadbackManifest) {
        ('"' + (Resolve-Path -LiteralPath $ReadbackManifest).Path.Replace('\', '/') + '"') |
            Add-Content -LiteralPath $argumentsFile -Encoding utf8NoBOM
    }
    & (Join-Path $JavaHome 'bin/java.exe') "@$argumentsFile"
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
