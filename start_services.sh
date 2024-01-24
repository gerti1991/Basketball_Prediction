source env/Scripts/activate
uvicorn src.FastApi.API:app --port 8000 &

sleep 10
deactivate

sleep 5
./ngrok start my_tunnel
# ngrok_output=$(./ngrok http 8000)
# ngrok_url=$(echo "$ngrok_output" | awk '/^http/ { print $NF }')
# ngrok_url="$ngrok_url/events-stats"

# curl -X POST "https://api.telegram.org/bot6313757405:AAESPj-GRvysErDK9Q6wMLg5nOjMk83z8TI/sendMessage" -d "chat_id=1447321557&text=Ngrok URL: $ngrok_url"

while true; do
  sleep 1
done
