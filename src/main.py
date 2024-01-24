import subprocess
import requests
import os


# Change the current directory to the parent directory
# os.chdir("..")

# Check the current directory again
# current_directory = os.getcwd()
# print("Current Directory after cd ..:", current_directory)


# Directory where your scripts are located
scripts_directory = "C:\\Users\\tiran\\Basketball_Predictions"
os.chdir(scripts_directory)

# Replace with your Telegram bot token and chat ID
TELEGRAM_BOT_TOKEN = '6313757405:AAESPj-GRvysErDK9Q6wMLg5nOjMk83z8TI'
TELEGRAM_CHAT_ID = '1447321557'

def send_telegram_message(message):
    """Sends a message to a Telegram chat."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    requests.post(url, data=data)

def run_script(script_name):
    """Runs a script and reports its success or failure."""
    # activate_env = r"env\Scripts\activate"  # Adjust if your virtual environment's path is different
    # command = f"cmd.exe /c \"{activate_env} && python {script_name}\""
    command = f"cmd.exe /c \"python {script_name}\""
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True, shell=True)
        script = script_name.replace("src\\","").replace(".py","")
        send_telegram_message(f"{script}: {result.stdout.strip()}")
        print(f"{script}: {result.stdout.strip()}")
    except subprocess.CalledProcessError as e:
        send_telegram_message(f"{script_name}: {e.stderr.strip()}")
        print(f"{script_name}: {e.stderr.strip()}")

# List of Python scripts to run
scripts = ['Upload_TS_PS_toMongo.py', 'final_BPM.py', 'spread_Players.py','prediction_BPM.py','predict_Market_Bet.py','status.py']

for script in scripts:
    run_script(f"src\\{script}")
