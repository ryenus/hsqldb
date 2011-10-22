@echo off

:: $Id$
:: author: Blaine Simpson of the HSQL Development Group
:: Distribution is permitted under the terms of the HSQLDB license.
:: (c) 2011 The HSQL Development Group

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=\.\

:: Change Java memory settings here:
set GRADLE_OPTS=-Xms256m -Xmx1g -XX:MaxPermSize=256m
::cd
::echo "DIRNAME=(%DIRNAME%)"
:: gradlew must be run from the HyperSQL build directory:
cd "%DIRNAME%"
::cd
type gui-welcome.txt

:: If there is no settings file in place, start user with our customized one:
if not exist gradle-app.setting copy gui-initial.setting gradle-app.setting > nul

.\gradlew.bat --gui

:: Enable following line for debugging purposes:
::pause
