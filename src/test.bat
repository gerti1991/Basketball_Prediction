@echo off
setlocal

:: Set the path to your virtual environment activation script
set VENV_ACTIVATION_SCRIPT=C:\Users\tiran\Basketball_Predictions\env\Scripts\activate
set VENV_DEACTIVATION_SCRIPT=C:\Users\tiran\Basketball_Predictions\env\Scripts\deactivate.bat

:: Set the paths to your Python scripts
set SCRIPT1=C:\Users\tiran\Basketball_Predictions\src\Upload_TS_PS_toMongo.py
set SCRIPT2=C:\Users\tiran\Basketball_Predictions\src\final_BPM.py
set SCRIPT3=C:\Users\tiran\Basketball_Predictions\src\spread_Players.py
set SCRIPT4=C:\Users\tiran\Basketball_Predictions\src\prediction_BPM.py
set SCRIPT5=C:\Users\tiran\Basketball_Predictions\src\predict_Market_Bet.py
set SCRIPT6=C:\Users\tiran\Basketball_Predictions\src\status.py

:: Define your Telegram bot token and chat ID
set TELEGRAM_BOT_TOKEN="6313757405:AAESPj-GRvysErDK9Q6wMLg5nOjMk83z8TI"
set TELEGRAM_CHAT_ID="1447321557"

:: Function to send a message to Telegram
:sendTelegramMessage
curl -s -X POST "https://api.telegram.org/bot%TELEGRAM_BOT_TOKEN%/sendMessage" ^
  -d "chat_id=%TELEGRAM_CHAT_ID%" ^
  -d "text=%1"
exit /b

:: Activate the virtual environment
call "%VENV_ACTIVATION_SCRIPT%"

:: Run your Python scripts and log errors
echo Running Script 1
python "%SCRIPT1%"
if %errorlevel% equ 0 (
    echo Script 1 executed successfully
) else (
    echo Script 1 encountered an error
    call :sendTelegramMessage "Script 1 encountered an error"
    call "%VENV_DEACTIVATION_SCRIPT%"
    goto :end
)

echo Running Script 2
python "%SCRIPT2%"
if %errorlevel% equ 0 (
    echo Script 2 executed successfully
) else (
    echo Script 2 encountered an error
    call :sendTelegramMessage "Script 2 encountered an error"
    call "%VENV_DEACTIVATION_SCRIPT%"
    goto :end
)

echo Running Script 3
python "%SCRIPT3%"
if %errorlevel% equ 0 (
    echo Script 3 executed successfully
) else (
    echo Script 3 encountered an error
    call :sendTelegramMessage "Script 3 encountered an error"
    call "%VENV_DEACTIVATION_SCRIPT%"
    goto :end
)

echo Running Script 4
python "%SCRIPT4%"
if %errorlevel% equ 0 (
    echo Script 4 executed successfully
) else (
    echo Script 4 encountered an error
    call :sendTelegramMessage "Script 4 encountered an error"
    call "%VENV_DEACTIVATION_SCRIPT%"
    goto :end
)

echo Running Script 5
python "%SCRIPT5%"
if %errorlevel% equ 0 (
    echo Script 5 executed successfully
) else (
    echo Script 5 encountered an error
    call :sendTelegramMessage "Script 5 encountered an error"
    call "%VENV_DEACTIVATION_SCRIPT%"
    goto :end
)

echo Running Script 6
python "%SCRIPT6%"
if %errorlevel% equ 0 (
    echo Script 6 executed successfully
) else (
    echo Script 6 encountered an error
    call :sendTelegramMessage "Script 6 encountered an error"
    call "%VENV_DEACTIVATION_SCRIPT%"
    goto :end
)

:end
:: Deactivate the virtual environment (in case it's still active)
call "%VENV_DEACTIVATION_SCRIPT%"

:: End of the script
echo All scripts executed.

endlocal
