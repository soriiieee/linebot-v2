# 様々なメッセージオブジェクト
from linebot.models import (
    MessageEvent, TextMessage, LocationMessage, TextSendMessage, QuickReplyButton, QuickReply,  MessageAction, ImageMessage, LocationAction, TemplateSendMessage, CarouselTemplate, CarouselColumn, PostbackAction, CarouselTemplate, URIAction
)

# HTTP通信と解析を行うライブラリ
from bs4 import BeautifulSoup
import requests
import json
import time