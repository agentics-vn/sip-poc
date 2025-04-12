## Installation

Make sure you have installed python virtualenv
```
python3 -m venv .venv
pip3 install -r requirements.txt
```

## Run server
```
python3 main.py
```

By default websocket server will listen at port 8000

## Listen from internet

To listen requests from internet to your localhost, setup and run ngrok
Reference: https://ngrok.com/downloads

```
ngrok http 8000
```

## Make call using cURL

```
curl --location '<psr url>' \
--header 'Authorization: Basic <token>' \
--form 'url="<your ngrok url>"' \
--form 'destination="<your phone>"' \
--form 'app="bot"' \
--form 'is_silence="true"' \
--form 'channel="mono_a"'

```

Where:

your_ngrok_url: get from ngrok console (eg: wss://13f1-113-161-95-53.ngrok-free.app/ws)
