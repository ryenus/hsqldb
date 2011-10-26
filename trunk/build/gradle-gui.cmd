@echo off

:: $Id$
:: author: Blaine Simpson of the HSQL Development Group
:: Distribution is permitted under the terms of the HSQLDB license.
:: (c) 2011 The HSQL Development Group

:: Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.

::cd
::echo "DIRNAME=(%DIRNAME%)"
:: gradlew must be run from the HyperSQL build directory:
cd "%DIRNAME%"
::cd

:: Eliminate confusing problems between ERRORLEVEL state and ERRORLEVEL var
set ERRORLEVEL=
:: Reset ERRORLEVEL state to 0:
ver > nul

if not "%JAVA_HOME%" == "" (
    rem Test JAVA_HOME if it is set
    rem echo JAVA_HOME is set
    rem echo Executing: "%JAVA_HOME%\bin\java" -version
    "%JAVA_HOME:/=\%\bin\java" -version > nul 2>&1
    if ERRORLEVEL 1 (
        echo:
        echo Please fix your JAVA_HOME env. variable.  Executable 'java' does not exist: '%JAVA_HOME:/=\%\bin\java.*'
        echo:
        pause
        exit /b
    )
)
if "%JAVA_HOME%" == "" (
    rem If JAVA_HOME not set, then test for 'java' executable
    java -version > nul 2>&1
    if ERRORLEVEL 1 (
        echo:
        echo You must set env. variable 'JAVA_HOME' or put 'java' into your search path.
        echo:
        pause
        exit /b
    )
)

type gui-welcome.txt

:: If there is no settings file in place, start user with our customized one:
if not exist gradle-app.setting copy gui-initial.setting gradle-app.setting > nul

call .\gradlew.bat --gui %*
:: ERRORLEVEL will be passed through to caller with no extra work here.

:: Give user an opportunity to see the error:
if ERRORLEVEL 1 pause
