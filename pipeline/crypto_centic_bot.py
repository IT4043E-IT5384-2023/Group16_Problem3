import io
from PIL import Image
from dotenv import load_dotenv
import os
from processing import *
from anomaly_detection import *
from check_scam import check_scam
from cluster_based_anomaly import *

from telegram.ext.updater import Updater
from telegram.update import Update
from telegram.ext.callbackcontext import CallbackContext
from telegram.ext.commandhandler import CommandHandler
from telegram.ext.messagehandler import MessageHandler
from telegram.ext.filters import Filters

load_dotenv()
TOKEN = os.getenv('TOKEN')

def start(update, context):
    update.message.reply_text("Welcome to Centic Crypto BOT!")

def echo(update, context):
    update.message.reply_text(update.message.text)
def check(update, context):
    addr = update.message.text.split(' ')[-1]
    update.message.reply_text("Check scam status for the address " + addr)
    msg = check_scam(addr)
    update.message.reply_text(msg)
def alert(update, context):
    chat_id = update.message.chat_id
    # path = r"D:\Documents\BigData\Group16_Problem5\data\final_etherium_token_transfer.csv"
    df_raw = load_data()
    df1 = anomaly_token_transaction(df_raw)
    df2 = anomaly_wallet_transaction(df_raw)
    df3 = mixing_service(df_raw)

    msg1 = f"Found {alert_msg(df1)} high-risk token transactions."
    update.message.reply_text(msg1)
    msg2 = f"Found {alert_msg(df2)} high-risk transactions associated each user."
    update.message.reply_text(msg2)
    msg3 = f"Found {alert_msg_mixing(df3)} mixing service."
    update.message.reply_text(msg3)
    url = "http://34.143.255.36:5601/s/it4043e---group16/app/dashboards#/view/f6f8a710-a13a-11ee-8d94-5d4fdf5aea4c?_g=(filters:!(),refreshInterval:(pause:!t,value:60000),time:(from:'2023-08-28T07:42:11.000Z',to:'2023-11-28T13:57:59.000Z'))"
    update.message.reply_text(f"Check out {url} for better visualization")
    # context.bot.send_photo(chat_id, photo=open(r'D:\Documents\BigData\Group16_Problem5\bot\Figure_1.png', 'rb'))
def help(update, context):
      update.message.reply_text(
      """
      /start -> Welcome to Centic Crypto BOT!
      /help -> To get the help menu.
      /echo -> echo to every message
      /check -> check scam for a given address
      /alert -> get alert of anomaly transaction
      """
      )
      
updater = Updater(token=TOKEN, use_context=True)
dp = updater.dispatcher

dp.add_handler(CommandHandler("start", start))
dp.add_handler(CommandHandler("echo", echo))
dp.add_handler(CommandHandler("help", help))
dp.add_handler(CommandHandler("check", check))
dp.add_handler(CommandHandler("alert", alert))

updater.start_polling()  # Start the bot
updater.idle() # Wait for the script to be stopped; this will stop the bot
