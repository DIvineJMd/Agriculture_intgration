@echo off
color 0A
mode con: cols=100 lines=30
title Agricultural Advisor System

:SHOW_HEADER
cls
echo ===============================================================================
echo                          Agricultural Advisor System
echo ===============================================================================
echo.

@REM :CHECK_DIRECTORIES
@REM echo [*] Checking required directories...
@REM if exist "ExtractAndLoad" (
@REM     echo [√] ExtractAndLoad directory found
@REM ) else (
@REM     echo [X] ExtractAndLoad directory not found
@REM     pause
@REM     exit /b 1
@REM )

@REM if exist "DataFetchingAndTransformationFromServer" (
@REM     echo [√] DataFetchingAndTransformationFromServer directory found
@REM ) else (
@REM     echo [X] DataFetchingAndTransformationFromServer directory not found
@REM     pause
@REM     exit /b 1
@REM )
@REM echo.

@REM :RUN_EXTRACTION
@REM echo ===============================================================================
@REM echo                              Data Extraction Phase
@REM echo ===============================================================================
@REM echo.

@REM echo [*] Running weather_data.py...
@REM python ExtractAndLoad\weather_data.py
@REM if errorlevel 1 (
@REM     echo [X] Failed to complete weather_data.py
@REM     pause
@REM     exit /b 1
@REM )
@REM echo [√] Successfully completed weather_data.py
@REM echo.

@REM echo [*] Running soilhealtdata.py...
@REM python ExtractAndLoad\soilhealtdata.py
@REM if errorlevel 1 (
@REM     echo [X] Failed to complete soilhealtdata.py
@REM     pause
@REM     exit /b 1
@REM )
@REM echo [√] Successfully completed soilhealtdata.py
@REM echo.

@REM :RUN_TRANSFORMATION
@REM echo ===============================================================================
@REM echo                           Data Transformation Phase
@REM echo ===============================================================================
@REM echo.

@REM set "scripts=cropDataTranformation.py fertilizer_data.py "Irrigated and crop transformation.py" soil_type.py soilData.py weatherTransformation.py"

@REM for %%s in (%scripts%) do (
@REM     echo [*] Running %%s...
@REM     if exist "DataFetchingAndTransformationFromServer\%%~s" (
@REM         python "DataFetchingAndTransformationFromServer\%%~s"
@REM         if errorlevel 1 (
@REM             echo [X] Failed to complete %%s
@REM             pause
@REM             exit /b 1
@REM         )
@REM         echo [√] Successfully completed %%s
@REM         echo.
@REM     ) else (
@REM         echo [X] File not found: DataFetchingAndTransformationFromServer\%%~s
@REM         pause
@REM         exit /b 1
@REM     )
@REM )

:SHOW_MENU
cls
echo ===============================================================================
echo                          Agricultural Advisor System
echo ===============================================================================
echo.
echo All data has been successfully extracted and transformed.
echo.
echo Please select an application to run:
echo.
echo [1] Crop Recommendation System
echo [2] Market Intelligence System
echo [3] Profit Maximization Analysis
echo [4] Smart Fertilizer Advisory
echo [5] LLM Integration
echo [6] Exit
echo.
echo ===============================================================================

set /p choice="Enter your choice (1-6): "

if "%choice%"=="1" (
    cls
    echo Running Crop Recommendation System...
    python Application\Crop_Recommendation_System.py
    pause
    goto SHOW_MENU
)
if "%choice%"=="2" (
    cls
    echo Running Market Intelligence System...
    python Application\Market_Intelligence_System.py
    pause
    goto SHOW_MENU
)
if "%choice%"=="3" (
    cls
    echo Running Profit Maximization Analysis...
    python Application\Profit_Maximization_Analysis.py
    pause
    goto SHOW_MENU
)
if "%choice%"=="4" (
    cls
    echo Running Smart Fertilizer Advisory...
    python Application\Smart_Fertilizer_Advisory.py
    pause
    goto SHOW_MENU
)
if "%choice%"=="5" (
    cls
    echo Running LLM Integration...
    python Application\LLM_integration.py
    pause
    goto SHOW_MENU
)
if "%choice%"=="6" (
    echo Thank you for using Agricultural Advisor System!
    timeout /t 3 >nul
    exit /b 0
)

echo Invalid choice! Please try again.
timeout /t 2 >nul
goto SHOW_MENU