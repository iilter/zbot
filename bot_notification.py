import requests
import json
from botclass import Trade as tradeClass
import botfunction as func

URL_TELEGRAM = 'https://api.telegram.org/bot'
TOKEN = '5474334107:AAEceU3EUiINChLTunuTsZ6CZN-udB3e_EY'

def getChatId():
    url = URL_TELEGRAM + TOKEN + '/getUpdates'
    response = requests.get(url)
    r = response.json()
    chatId = r['result'][0]['message']['chat']['id']
    return str(chatId)

def sendNotification(notification=None):
    bot_chatID = getChatId()
    msg = f"Hesap Özeti\n {notification}"
    send_text = URL_TELEGRAM + TOKEN + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + msg
    response = requests.get(send_text)
    return response.json

def readSummary():
    db = func.connectDB()
    dbCursor = db.cursor()

    trade = tradeClass()
    res = trade.readProfitSummary(dbCursor=dbCursor)

    return res

def main():
    row = readSummary()
    record = json.loads(row[0])
    kar = record["kar"]
    zarar = record["zarar"]
    fark = record["fark"]
    message = f"Kar: {kar}\n Zarar: {zarar}\n Fark: {fark}"
    r = sendNotification(notification=message)

# if __name__ == "__main__":
main()
