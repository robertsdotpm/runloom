@echo off
REM install.bat -- detect / bootstrap / build / install pygo on Windows.
REM
REM Mirrors scripts/install.sh.  Uses cmd.exe directly so it works on
REM stock Windows installs without any PowerShell policy fuss.

setlocal EnableExtensions EnableDelayedExpansion
set "SCRIPT_DIR=%~dp0"
set "REPO_DIR=%SCRIPT_DIR%.."
if "%PYTHON%"=="" set "PYTHON=python"

REM 1. Python check.
"%PYTHON%" --version >nul 2>&1
if errorlevel 1 (
    echo [install] no python on PATH ^(tried "%PYTHON%"^); install Python 3.11+ first
    exit /b 1
)

REM 2. Version gate.
"%PYTHON%" -c "import sys; sys.exit(0 if sys.version_info>=(3,11) else 1)"
if errorlevel 1 (
    echo [install] pygo requires Python 3.11+
    exit /b 1
)

REM 3. Compiler probe: try cl, gcc, clang on PATH; if none, bootstrap.
where cl    >nul 2>&1 && goto have_cc
where gcc   >nul 2>&1 && goto have_cc
where clang >nul 2>&1 && goto have_cc

REM vswhere can find Build Tools that aren't on PATH but distutils can
REM still locate them.  Probe that too before giving up.
set "VSWHERE=%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe"
if exist "%VSWHERE%" (
    "%VSWHERE%" -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath >nul 2>&1
    if not errorlevel 1 goto have_cc
)

echo [install] no Windows C compiler detected; running bootstrap_compiler.bat
call "%SCRIPT_DIR%bootstrap_compiler.bat"
if errorlevel 1 (
    echo [install] compiler bootstrap failed; install MSVC Build Tools or MinGW-w64 manually
    exit /b 2
)

:have_cc

REM 4. pip / setuptools.
"%PYTHON%" -m pip --version >nul 2>&1
if errorlevel 1 (
    echo [install] pip missing; running ensurepip
    "%PYTHON%" -m ensurepip --upgrade
    if errorlevel 1 (
        echo [install] ensurepip failed; install pip manually
        exit /b 3
    )
)
"%PYTHON%" -m pip install --upgrade --quiet pip setuptools wheel

REM 5. Install.
echo [install] running pip install %* .
cd /d "%REPO_DIR%"
"%PYTHON%" -m pip install %* .
if errorlevel 1 exit /b 4

REM 6. Sanity-check.
"%PYTHON%" -c "import pygo_core; print('coro=', pygo_core.backend(), ' netpoll=', pygo_core.netpoll_backend())"
echo [install] done
endlocal
