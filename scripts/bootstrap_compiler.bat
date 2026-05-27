@echo off
REM bootstrap_compiler.bat -- shim that hands off to PowerShell.
REM
REM cmd.exe can't reliably download files or unpack zips, so all the real
REM work lives in bootstrap_compiler.ps1.  This wrapper exists so users
REM who land in cmd (the default Windows shell) don't have to know that.

setlocal
where pwsh >nul 2>&1
if %errorlevel%==0 (
    set "PSEXE=pwsh"
) else (
    set "PSEXE=powershell"
)

%PSEXE% -NoProfile -ExecutionPolicy Bypass -File "%~dp0bootstrap_compiler.ps1" %*
exit /b %errorlevel%
