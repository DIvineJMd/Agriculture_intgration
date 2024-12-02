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

:CHECK_DIRECTORIES
echo [*] Checking required directories...
if exist "ExtractAndLoad" (
    echo [√] ExtractAndLoad directory found
) else (
    echo [X] ExtractAndLoad directory not found
    pause
    exit /b 1
)

if exist "DataFetchingAndTransformationFromServer" (
    echo [√] DataFetchingAndTransformationFromServer directory found
) else (
    echo [X] DataFetchingAndTransformationFromServer directory not found
    pause
    exit /b 1
)
echo.

:RUN_EXTRACTION
echo ===============================================================================
echo                              Data Extraction Phase
echo ===============================================================================
echo.

echo [*] Running weather_data.py...
python ExtractAndLoad\weather_data.py
if errorlevel 1 (
    echo [X] Failed to complete weather_data.py
    pause
    exit /b 1
)
echo [√] Successfully completed weather_data.py
echo.

echo [*] Running soilhealtdata.py...
python ExtractAndLoad\soilhealtdata.py
if errorlevel 1 (
    echo [X] Failed to complete soilhealtdata.py
    pause
    exit /b 1
)
echo [√] Successfully completed soilhealtdata.py
echo.

:RUN_TRANSFORMATION
echo ===============================================================================
echo                           Data Transformation Phase
echo ===============================================================================
echo.

set "scripts=cropDataTranformation.py fertilizer_data.py "Irrigated and crop transformation.py" soil_type.py soilData.py weatherTransformation.py"

for %%s in (%scripts%) do (
    echo [*] Running %%s...
    if exist "DataFetchingAndTransformationFromServer\%%~s" (
        python "DataFetchingAndTransformationFromServer\%%~s"
        if errorlevel 1 (
            echo [X] Failed to complete %%s
            pause
            exit /b 1
        )
        echo [√] Successfully completed %%s
        echo.
    ) else (
        echo [X] File not found: DataFetchingAndTransformationFromServer\%%~s
        pause
        exit /b 1
    )
)

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