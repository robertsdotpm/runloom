# bootstrap_compiler.ps1 -- Windows compiler bootstrapper for pygo.
#
# Strategy: prefer the MSVC Build Tools (best Python wheel compatibility),
# fall back to MinGW-w64 (smaller download, works on Win 7+), then clang.
# Skip everything if a usable compiler is already on PATH.
#
# Usage (PowerShell, as admin or with -Force to silence the elevation hint):
#   .\scripts\bootstrap_compiler.ps1
#   .\scripts\bootstrap_compiler.ps1 -Toolchain mingw
#   .\scripts\bootstrap_compiler.ps1 -Toolchain msvc
#
# -Toolchain auto (default): try MSVC, then MinGW
# -Toolchain msvc:           install Visual Studio Build Tools 2022 (~2 GB)
# -Toolchain mingw:          install MinGW-w64 via WinLibs zip (~300 MB)

[CmdletBinding()]
param(
    [ValidateSet("auto","msvc","mingw","clang")]
    [string]$Toolchain = "auto",
    [switch]$Force
)

$ErrorActionPreference = "Stop"

function Test-Command($name) {
    $null = Get-Command $name -ErrorAction SilentlyContinue
    return $?
}

function Log($msg) { Write-Host "[bootstrap] $msg" }

# --- Detect already-installed compilers ---------------------------------
if (Test-Command "cl.exe")  { Log "MSVC cl.exe already on PATH";  exit 0 }
if (Test-Command "gcc.exe") { Log "MinGW gcc.exe already on PATH"; exit 0 }
if (Test-Command "clang.exe") { Log "clang.exe already on PATH"; exit 0 }

# Also probe the standard MSVC install locations even if cl.exe isn't
# on PATH; "vswhere" can find Build Tools that haven't been added to PATH.
$vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
if (Test-Path $vswhere) {
    $msvc = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath 2>$null
    if ($msvc) {
        Log "MSVC found at $msvc (not on PATH; use a Developer Command Prompt)"
        Log "tip: pip install will still find it via the distutils MSVC autodetect"
        exit 0
    }
}

Log "no Windows C compiler detected; installing toolchain '$Toolchain'"

# --- Helper: download a file with progress ------------------------------
function Get-File($Url, $Dest) {
    Log "downloading $Url"
    $progress = $ProgressPreference
    $ProgressPreference = "SilentlyContinue"   # 50x faster downloads
    try {
        Invoke-WebRequest -Uri $Url -OutFile $Dest -UseBasicParsing
    } finally {
        $ProgressPreference = $progress
    }
}

# --- MSVC Build Tools install -------------------------------------------
function Install-MSVC {
    $url = "https://aka.ms/vs/17/release/vs_BuildTools.exe"
    $exe = Join-Path $env:TEMP "vs_BuildTools.exe"
    Get-File $url $exe
    Log "running MSVC Build Tools installer (this takes 5-15 min, ~2 GB)"
    # --quiet --wait runs unattended.  Components selected:
    #   - VC tools x64 (cl.exe, link.exe, ATL/MFC stripped to keep small)
    #   - Windows 10/11 SDK (latest)
    $args = @(
        "--quiet", "--wait", "--norestart",
        "--add", "Microsoft.VisualStudio.Workload.VCTools",
        "--add", "Microsoft.VisualStudio.Component.VC.Tools.x86.x64",
        "--add", "Microsoft.VisualStudio.Component.Windows11SDK.22621",
        "--includeRecommended"
    )
    & $exe @args
    if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne 3010) {
        throw "MSVC installer failed with exit $LASTEXITCODE"
    }
    Log "MSVC Build Tools installed; you may need to restart the shell"
}

# --- MinGW-w64 install via WinLibs --------------------------------------
function Install-MinGW {
    # WinLibs.com hosts curated GCC builds with bundled mingw-w64 runtime.
    # We pin a known-good UCRT build to avoid surprise behaviour changes.
    $url  = "https://github.com/brechtsanders/winlibs_mingw/releases/download/13.2.0posix-17.0.6-11.0.1-ucrt-r1/winlibs-x86_64-posix-seh-gcc-13.2.0-mingw-w64ucrt-11.0.1-r1.zip"
    $zip  = Join-Path $env:TEMP "winlibs-mingw.zip"
    $dest = Join-Path $env:ProgramFiles "mingw64"
    Get-File $url $zip
    Log "extracting MinGW-w64 to $dest"
    if (Test-Path $dest) { Remove-Item -Recurse -Force $dest }
    Expand-Archive -Path $zip -DestinationPath (Split-Path $dest) -Force
    # WinLibs zip extracts to "mingw64/" at the root; ProgramFiles already
    # contains that subdir name after extraction.
    $bin = Join-Path $dest "bin"
    if (-not (Test-Path (Join-Path $bin "gcc.exe"))) {
        throw "MinGW extracted but gcc.exe not at expected path"
    }
    # Add to user PATH (idempotent).
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($userPath -notlike "*$bin*") {
        [Environment]::SetEnvironmentVariable("Path", "$userPath;$bin", "User")
        Log "added $bin to user PATH (re-open shell to pick up)"
    }
    $env:Path = "$env:Path;$bin"
    Log "MinGW-w64 installed; gcc --version:"
    & "$bin\gcc.exe" --version
}

# --- LLVM/clang install via official MSI ---------------------------------
function Install-Clang {
    $url = "https://github.com/llvm/llvm-project/releases/download/llvmorg-17.0.6/LLVM-17.0.6-win64.exe"
    $exe = Join-Path $env:TEMP "LLVM-installer.exe"
    Get-File $url $exe
    Log "running LLVM/clang installer (~400 MB)"
    & $exe "/S" "/D=$env:ProgramFiles\LLVM"
    if ($LASTEXITCODE -ne 0) { throw "LLVM installer failed exit $LASTEXITCODE" }
    $bin = "$env:ProgramFiles\LLVM\bin"
    $env:Path = "$env:Path;$bin"
    Log "clang installed; clang --version:"
    & "$bin\clang.exe" --version
}

# --- Dispatch -----------------------------------------------------------
switch ($Toolchain) {
    "msvc"  { Install-MSVC }
    "mingw" { Install-MinGW }
    "clang" { Install-Clang }
    "auto" {
        try {
            Install-MSVC
        } catch {
            Log "MSVC install failed ($_); trying MinGW-w64"
            Install-MinGW
        }
    }
}

Log "done"
