import json
# import requests
import os
import sys 
# from linebot import (LineBotApi, WebhookHandler)
# from linebot.exceptions import (LineBotApiError, InvalidSignatureError)
# from linebot.models import (MessageEvent, TextMessage, TextSendMessage,)

from utils.geocoding import Geocode,HinanPoint

from pathlib import Path
cwd = Path(__file__).parent
sys.path.append(str(cwd))
from env import ENV

# print("YOUR_CHANNEL_ACCESS_TOKEN",YOUR_CHANNEL_ACCESS_TOKEN)
# print("YOUR_CHANNEL_SECRET",YOUR_CHANNEL_SECRET)


def lambda_handler(event, context):
    YOUR_CHANNEL_ACCESS_TOKEN = os.environ["YOUR_CHANNEL_ACCESS_TOKEN"]
    YOUR_CHANNEL_SECRET = os.environ["YOUR_CHANNEL_SECRET"]
    # 環境変数取得
    line_bot_api = LineBotApi(YOUR_CHANNEL_ACCESS_TOKEN)
    handler = WebhookHandler(YOUR_CHANNEL_SECRET)

    signature = event["headers"]['x-line-signature'] # get X-Line-Signature header value
    body = event["body"] #get request body as text
    print("Request body: " + body)

    @handler.add(MessageEvent, message=TextMessage)
    def message(line_event):
        text = line_event.message.text
        line_bot_api.reply_message(
            line_event.reply_token, TextSendMessage(text=text))

    try:
        handler.handle(body, signature)
    except LineBotApiError as e:
        logger.error("Got exception from LINE Messaging API: %s\n" % e.message)
        for m in e.error.details:
            logger.error("  %s: %s" % (m.property, m.message))

    except InvalidSignatureError:
        logger.error("sending message happen error")


def test():
    
    # address= "東京都板橋区上板橋"
    # gmap = Geocode(address)
    # info = gmap.get_latlon()
    
    info = tuple([35.7640095, 139.6719963, '13'])
    if info[2] is None: exit(1)
     
    environment = os.getenv("GOOGLE_API_KEY","development")
    aws_config = {
        "AWS_ACCESS_KEY_ID" : os.getenv("AWS_ACCESS_KEY_ID",ENV.AWS_ACCESS_KEY_ID),
        "AWS_SECRET_ACCESS_KEY" : os.getenv("AWS_SECRET_ACCESS_KEY",ENV.AWS_SECRET_ACCESS_KEY),
        "AWS_REGION" : os.getenv("AWS_REGION",ENV.AWS_REGION),
    }
    hp = HinanPoint(environment,aws_config,info)
    hp.read_shp_csv_to_s3bucket()
    
if __name__ == "__main__":
    test()