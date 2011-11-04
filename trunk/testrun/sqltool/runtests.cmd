
:: @author Blaine Simpson

:: Wrapper that serves two purposes:
:: (1) Takes traditional parameters in format: command -switches... filenames...
::     and converts to a Gradle command of format: Gradle -P x=y...  taskname
:: (2) Invokes the 'gradlew' wrapper script supplied by the HyperSQL
::     distribution.

@echo off

if "%OS%"=="Windows_NT" setlocal 
set ERRORLEVEL=
ver > nul

if "%1" == "-h" (
    set VERBOSE=-Phelp=true
    shift
) else if "%1" == "-v" (
    set VERBOSE=-Pverbose=true
    shift
) else if "%1" == "-n" (
    set NORUN=-Pnorun=true
    shift
) else if "%1" == "-nv" (
    set VERBOSE=-Pverbose=true
    set NORUN=-Pnorun=true
    shift
) else if "%1" == "-vn" (
    set VERBOSE=-Pverbose=true
    set NORUN=-Pnorun=true
    shift
)

set SCRIPTLIST=
:moreArgs
    if "%1" == "" goto noMoreArgs
    if not "%SCRIPTLIST%" == "" (
        set SCRIPTLIST=%SCRIPTLIST%,%1
    ) else (
        set SCRIPTLIST=-Pscripts=%1
    )
    shift
goto moreArgs

:noMoreArgs

if "%VERBOSE%" == "true" echo ..\..\build\gradlew %HELP% %VERBOSE% %NORUN% %SCRIPTLIST%
..\..\build\gradlew %HELP% %VERBOSE% %NORUN% %SCRIPTLIST%
:: People who have a real, local Gradl installation can use the following:
::gradle %HELP% %VERBOSE% %NORUN% %SCRIPTLIST%
