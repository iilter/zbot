import urllib.parse

URL_TELEGRAM = "https://api.telegram.org/bot"
# TOKEN = '5474334107:AAEceU3EUiINChLTunuTsZ6CZN-udB3e_EY' ## iilter_bot
TOKEN = "5668607592:AAHPAG4ugrgIysz70Z8GZcCeAXYw_Wpf8E4" #zbotii_bot
CHANNEL_ID = "-1001975813168"

def getChatId(connSession=None):
    url = URL_TELEGRAM + TOKEN + "/getUpdates"
    response = connSession.get(url)
    r = response.json()
    chatId = r['result'][0]['message']['chat']['id']
    return str(chatId)

def sendNotification(connSession=None, notification=None):
    # bot_chatID = getChatId(connSession=connSession)
    msg = urllib.parse.quote(notification)
    # send_text = URL_TELEGRAM + TOKEN + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + msg
    # zbot
    # send_text = f"{URL_TELEGRAM}{TOKEN}/sendMessage?chat_id={bot_chatID}&parse_mode=Markdown&text={msg}"
    # response = connSession.get(send_text)
    # zbot_channel
    send_text = f"{URL_TELEGRAM}{TOKEN}/sendMessage?chat_id=-1001975813168&parse_mode=Markdown&text={msg}"
    response = connSession.get(send_text)
    return response.json