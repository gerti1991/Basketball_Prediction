# Basketball_Prediction
1) in CMD fo to main file directory CD /mainDirectory
2) Install python -m venv env
3) Activate env env\Scripts\activate
4) Install dependency libraries pip install -r requirements.txt
5) main.py change the full directory of the project folder path variable called  "scripts_directory"
6) in run_scripts.bat change the path of env activation and main.py script path with your current one. This is the running task daily
7) Configure connectors.py it is using mongo database according to your mongo for example: client = MongoClient('mongodb://localhost:27017/')
8) In Upload_TS_PS_toMongo.py configure the API urls for player_stats,team_stats,event_stats
9) In predict_Market_bet.py update the url of api for market_bet info
10) In the cmd ngrok config add-authtoken ngrokApiToken go to link of the file of ngrok.yml
11) Config ngrok.yml by adding:
tunnels:
  my_tunnel:
    proto: http
    addr: 8000
    hostname: yourNgrokUrl
12) In power shell start webserver "uvicorn src.FastApi.API:app --port 8000" see if webserver ran succesfull and go to:
13) In a new powershell type ".\ngrok start my tunnel" and this will be the web link for API
14) "yourNgrokUrl/events-stats" is the link where you get the predictions to use for data analysis
