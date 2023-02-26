import copy
import logging
import random
import requests
import time
import asyncio
import json
import os.path
import os
from datetime import datetime
# import numpy as np
import pandas as pd
# import statsmodels.api as sm
from binance import Client
from binance.enums import HistoricalKlinesType
from binance import AsyncClient, BinanceSocketManager
from creds import api_key, api_secret, bot_token, chat_id
from futures_sign import send_signed_request

symbol = 'UNFIUSDT'
positionSide = 'LONG'
commision = float(0.0004)
interval = Client.KLINE_INTERVAL_5MINUTE
# cl = Client(key, secret) # Binance Client for Klines
# client = Client(api_key, api_secret, testnet=True)  # testnet Client
client = Client(api_key, api_secret)  # futures Client

stop_percent = float(0.001)  # 0.01=1% !!! Проверить на 537 строке
plech = int(10)  # Credit Plecho
temp = float(stop_percent * 20)  # Тейк профит
okrug = int(3)  # До скольки знаков после запятой округлять цену в USDT
pos_okrug = int(1)  # До скольки знаков после запятой округлять сумму в монетах.
chsti_contrakta = int(7)  # Сколькими частями закрывать позицию.
temp_l = float(0.7)  # Части закрытия позиции, задается в процентах
papka = f"data{symbol}_long"  # Папка, в которой лежит бот


"""
Блок работы телеграм Бота.
"""
pointer = str(random.randint(1000, 9999))
telegram_token = bot_token
telegram_chat_id = chat_id
telegram_base_url = f'https://api.telegram.org/bot{telegram_token}'
# Function to send a message to Telegram
def send_message(text):
    url = f'{telegram_base_url}/sendMessage'
    data = {'chat_id': telegram_chat_id, 'text': text}
    response = requests.post(url, data=data)
    if not response.ok:
        print(f'Error sending message to Telegram: {response.status_code} {response.text}')

# Flag to indicate whether to stop the script or not
stop_script = False

# Function to handle messages from Telegram
def handle_message(message):
    global stop_script
    text = message['text'].lower()
    if text == '/stop':
        stop_script = True
        send_message('Stopping script...')
        raise SystemExit

# Function to get updates from Telegram
def get_updates(offset=None):
    url = f'{telegram_base_url}/getUpdates'
    params = {'timeout': 30, 'offset': offset}
    response = requests.get(url, params=params)
    if response.ok:
        return response.json()
    print(f'Error getting updates from Telegram: {response.status_code} {response.text}')
    return []

# Function to process recent messages from Telegram
def process_messages(last_update_id=None):
    global last_message_time
    updates = get_updates(last_update_id)
    if updates:
        for update in updates['result']:
            if 'message' in update:
                message_time = update['message']['date']
                if time.time() - message_time < 15: # check if message is less than 15 second old
                    handle_message(update['message'])
                    last_message_time = message_time

def prt(message):
    # telegram message
    send_message(f'{pointer}: {message}')
    print(pointer + ': ' + message)

"""
Блок работы телегрпм Бота закончился.
"""

# Чтение из файла
def fileRead(file: str):
    file = open(papka + "/" + file + ".txt", "r")
    # file = open(file + ".txt", "r")
    te = json.loads(file.read())
    file.close()
    return te


# Записьв файл
def recordFile(file: str, proffit_array: str):
    my_file = open(file + ".txt", "w")
    my_file.writelines(proffit_array)
    my_file.close()
    return my_file

def number_red(max_min):
    l_min = round(max_min[1], okrug)
    l_max = round(max_min[0], okrug)
    mnogitel = 1
    rez = []
    for m in range(0, okrug):
        mnogitel *= 10  # Множитель для круглых чисел
    numb = int(l_min * mnogitel)
    max_numb = int(l_max * mnogitel)
    l = int(f"{str(numb)[:-1]}0")
    # print(l)
    while True:
        if l > max_numb:
            break
        l += 5
        rez.append(l / mnogitel)
        m = l - 1
        rez.append(m / mnogitel)
        o = l + 1
        rez.append(o / mnogitel)
    return rez


def diapozon():
    # функция задает диапозон, в котором ходит цена. Для ANTUSDT хватает 0.5. Индивидуально для каждой монеты.
    # file = open(papka + "/diapozon/diapozon.txt", "r")  # Ценовые уровни лежат в отдельном файле
    # red = json.loads(file.read())
    # file.close()

    x = client.get_historical_klines(symbol=symbol, interval=client.KLINE_INTERVAL_1DAY, limit=1,
                                     klines_type=HistoricalKlinesType.FUTURES)
    k_dey_high = float(x[0][2])
    k_dey_low = float(x[0][3])
    # print(f"high {k_dey_high} low {k_dey_low}")
    max_min = [k_dey_high + 0.5, k_dey_low - 0.5]
    # Берем диапозон по числам, которые кратны 5-ти.
    red = number_red(max_min)
    return red


up = diapozon()


def cena():
    cons = client.futures_order_book(symbol=symbol, limit=5)
    dc = pd.DataFrame(cons)
    cena = float(dc.loc[4].at["asks"][0])
    return cena


cen = round(cena() * temp, okrug)


def maxposition(dep):
    pl = plech  # Плечо
    if dep < float(10):
        nd = float(10)  # Стартовый депозит
        recordFile(file='dep_1000SHIB', proffit_array=str(nd))
    else:
        nd = float(dep)  # Стартовый депозит
    balance = round(nd * pl, 2)
    # Цена одной монеты
    coins = client.futures_order_book(symbol=symbol, limit=5)
    df = pd.DataFrame(coins)
    coins_cena = float(df.loc[4].at["asks"][0])
    # Бюджет на торговлю в долларах
    if nd > 2500:
        balance = 2500
    maxposition = float(round(balance / coins_cena, pos_okrug))
    return maxposition, coins_cena


# Функия подсчета контрактов
def summ_contracts(eth_proffit_array):
    b = 0
    for ty in eth_proffit_array:
        b += ty[1]
    # print('Kontraktov: ',b)
    return b


# PNL
def pnl_comm(symbol, up_time, orderId):
    a = client.futures_get_all_orders(symbol=symbol, startTime=f"{int(str(up_time)[:-3])}000")
    pnl_round_num = 8  # Округлить число, 8 знаков после запятой.
    if len(a) > 0:
        arr_pnl = []
        ar_qt = []
        arr_out = []
        arr_com = []
        ar_in_pr = []
        for cn in a:
            if cn['status'] == 'FILLED' and cn['positionSide'] == positionSide and cn['side'] == 'BUY':
                # print(cn)
                com = round(float(cn['avgPrice']) * float(cn['origQty']) * commision, pnl_round_num) * (-1)
                arr_com.append(com)
                ar_in_pr.append(float(cn['avgPrice']))  # Собрать цену входа по всем ордерам.
        if len(ar_in_pr) > 0:
            pr_in = round(sum(ar_in_pr) / len(ar_in_pr), pnl_round_num)  # Получаем среднюю цену входа
            pn = orderId
            if pn['status'] == 'FILLED' and pn['positionSide'] == positionSide and pn['side'] == 'SELL':
                # print(pn)
                com = round(float(pn['avgPrice']) * float(pn['origQty']) * commision, pnl_round_num) * (-1)
                pnl = [float(pn['avgPrice']), float(pn['origQty'])]  # Собрать цену входа и количество по всем ордерам.
                arr_pnl.append(pnl)
                arr_com.append(com)
            # print("arr_com: ", arr_com, "comm: ", round(sum(arr_com), 8), "arr_pnl: ", arr_pnl, "ar_in_pr: ",ar_in_pr)
            for d in arr_pnl:
                arr_out.append(float(d[0]))
                ar_qt.append(float(d[1]))
            pr_out = round(sum(arr_out) / len(arr_out), pnl_round_num)  # Получаем среднюю цену выхода
            qt = round(sum(ar_qt), pnl_round_num)
            comm = round(sum(arr_com), pnl_round_num)
            fin = (qt * pr_out - qt * pr_in) + comm
            d_d = fileRead('dep_1000SHIB')
            recordFile('dep_1000SHIB', str(round(d_d + fin, pnl_round_num)))


# Get last 500 kandels 5 minutes for Symbol
def get_kline_period(symbol: str, interval: str, limit: int):
    # print('Pisition')
    x = client.get_historical_klines(symbol=symbol, interval=interval, limit=limit,
                                     klines_type=HistoricalKlinesType.FUTURES)
    return x


def get_futures_klines(kline_period):
    df = pd.DataFrame(kline_period)
    df.columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'd1', 'd2', 'd3', 'd4', 'd5']
    df = df.drop(['d1', 'd2', 'd3', 'd4', 'd5'], axis=1)
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)
    # Define moving average length
    ma_length = 100

    # Calculate moving average
    df['ma'] = df['close'].astype(float).rolling(window=ma_length).mean()
    return (df)


#
# Open position for Sybol with

def open_position_long(symbol, s_l, quantity_l):
    # Open long position
    prt('open: ' + symbol + ' quantity: ' + str(quantity_l))

    if (s_l == 'long'):
        params = {
            "batchOrders": [
                {
                    "symbol": symbol,
                    "side": "BUY",
                    "positionSide": positionSide,
                    "type": "MARKET",  # LIMIT
                    "quantity": str(quantity_l),
                    # "timeInForce": "GTC",
                    # "price": close_price

                }
            ]
        }
        responce = send_signed_request('POST', '/fapi/v1/batchOrders', params)
        print(responce)
        return responce


def stop_sell_long(symbol, s_l, k):
    # STOP SELL
    if (s_l == 'long'):
        # posit = get_opened_positions(symbol, okrug)
        maxpositio = maxposition(fileRead('dep_1000SHIB'))
        recordFile('maxpositio', str(maxpositio[0]))
        inr = float(k)#float(fileRead('k'))
        delta_price = round(inr * stop_percent, okrug)
        st = str(round(inr - delta_price, okrug))
        a = len(st.rpartition('.')[2])
        if a == okrug - 1:
            st = f"{float(st)}0"
        if a == okrug - 2:
            st = f"{float(st)}00"
        if isInt(float(st)) == True:  # Если целое число
            st = f"{float(st)}00"
        params = {
            "batchOrders": [
                {
                    "symbol": symbol,
                    "side": "SELL",
                    "positionSide": positionSide,
                    "type": "STOP_MARKET",  # LIMIT
                    "quantity": str(maxpositio[0]),
                    "stopPrice": st,
                    "closePosition": 'true'
                    # "timeInForce": "GTC",
                    # "price": close_price

                }
            ]
        }
        responce_buy = send_signed_request('POST', '/fapi/v1/batchOrders', params)
        print(responce_buy)
    else:
        responce_buy = 'ERROR'

    return responce_buy


# Close position for symbol with quantity

def close_position(symbol, s_l, quantity_l, start):
    prt('close: ' + symbol + ' quantity: ' + str(quantity_l))

    if (s_l == 'long'):
        params = {
            "symbol": symbol,
            "side": "SELL",
            "positionSide": positionSide,
            "type": "MARKET",
            "quantity": str(quantity_l),
            # "timeInForce": "GTC",
            # "price": close_price
        }
        responce = send_signed_request('POST', '/fapi/v1/order', params)
        print(responce)
        # Считаем PNL.
        while True:
            check_fil = client.futures_get_order(symbol=symbol, orderId=responce['orderId'])
            if check_fil['status'] == 'FILLED' and start == 1:
                pnl_comm(symbol=symbol, up_time=fileRead('time_for_PNL'), orderId=check_fil)
                break
            if start == 0:
                break
            time.sleep(1)
        return responce


# Find all opened positions

def get_opened_positions(symbol, okrug):
    status = client.futures_account()
    tf = status['positions']
    a = 0
    # print(status['positions'])
    for df in tf:
        if df['symbol'] == symbol and df['positionSide'] == positionSide:
            # print(df)
            a = float(df['positionAmt'])
            # print(positions[positions['symbol'] == symbol]['leverage'])
            leverage = int(df['leverage'])
            entryprice = float(df['entryPrice'])
    profit = float(status['totalUnrealizedProfit'])
    balance = round(float(status['totalWalletBalance']), 2)
    if a > 0:
        pos = "long"
    elif a < 0:
        pos = "short"
    else:
        pos = ""
    return ([pos, a, profit, leverage, balance, round(float(entryprice), okrug), 0])


# Close all orders

def check_and_close_orders(symbol: str):
    global isStop
    df = []
    a = client.futures_get_open_orders(symbol=symbol)
    print(a)
    if len(a) > 0:
        for t in a:
            if t.get('positionSide') == 'LONG':
                df.append(t)
        if len(df) > 0:
            for r in df:
                isStop = False
                client.futures_cancel_order(symbol=symbol, orderId=r['orderId'])


def isInt(n):
    # Если целое число - вернет True, в противном случае - False
    return int(n) == float(n)


def get_time_sleep(frame_time: int):
    # После открытия позиции ожидаем 5 мин.
    rt = str(time.time())
    rt = rt.split('.')
    # print(rn[0])
    current_datetime = datetime.fromtimestamp(int(rt[0]))
    time_min = current_datetime.minute
    time_sec = current_datetime.second
    # print('Сплю ', 300 - time_sec)
    # print(isInt(time_min / 5))
    if isInt(time_min / frame_time) == False:
        if len(str(time_min)) > 1:
            time_min = int(str(time_min)[-1:])
            # print(time_min)
            if time_min > frame_time:
                time_min = ((frame_time * 2) - time_min) * 60 - time_sec
                # print(time_min)
            else:
                time_min = (frame_time - time_min) * 60 - time_sec
                # print(time_min)
        else:
            time_min = int(time_min)
            # print(time_min)
            if time_min > frame_time:
                time_min = ((frame_time * 2) - time_min) * 60 - time_sec
                # print(time_min)
            else:
                time_min = (frame_time - time_min) * 60 - time_sec
                # print(time_min)
    else:
        time_min = 60 * frame_time - time_sec
        # print(time_min)
    return time_min

def check_if_signal(step):
    # Поиск сигнала для входа в позицию
    while True:
        process_messages()
        rn = str(time.time())
        rn = rn.split('.')
        #print(rn[0])
        current_datetime = datetime.fromtimestamp(int(rn[0]))
        time_sec = current_datetime.second
        if time_sec == 0 or step == 1:
            time.sleep(random.randint(3,7))
            prt('Нет открытых позиций ' + str(symbol))
            signal = ""  # return value
            # pu_h = []
            # pu_l = []
            kline_period = get_kline_period(symbol, interval, limit=101)
            # print(kline_period)
            df = get_futures_klines(kline_period)
            lend = len(df)
            """
            for i in range(0, lend - 1):
                # Ищим средние по размеру свечи
                if (df.loc[i].at['low'] / df.loc[i].at['high']) > 0.8:
                    pu_h.append(df.loc[i].at['high'])
                    pu_l.append(df.loc[i].at['low'])
            # Определем в какой части, дневного канала находится цена.
            max_uroven = sum(pu_h) / len(pu_h)
            min_uroven = sum(pu_l) / len(pu_l)
            uroven_in_c = (max_uroven - df.loc[lend - 2].at['close']) / (max_uroven - min_uroven)
            uroven_in_o = (max_uroven - df.loc[lend - 2].at['open']) / (max_uroven - min_uroven)
            """
            red = up
            # print(sorted(red))
            long = 3
            num_sec_sleep = 2 # Интервал между запросами.
            for k in red:
                # Ищим ложный пробой уровня.
                if (df.loc[lend - 2].at['close'] < k and df.loc[lend - 2].at['open'] > k) and (
                        df.loc[lend - 3].at['open'] > df.loc[lend - 3].at['close']):
                    if (df.loc[lend - 2].at['close'] < k) and (df.loc[lend - 2].at['close'] < df.loc[lend - 2].at['ma']):

                        l = 0
                        while True:
                            time_kline = get_time_sleep(frame_time=5)
                            ch = client.futures_klines(symbol=symbol, interval=interval, limit=1)
                            price_close = float(ch[0][4])

                            if (df.loc[lend - 2].at['low'] > price_close) or (time_kline < 10):
                                # Если цена упала ниже - то щеключаемся
                                signal = False
                                break

                            if price_close > k and price_close <= (k * (1 + stop_percent)): # Вход в сделку.
                                # Stop Sell
                                try:
                                    sll = stop_sell_long(symbol, 'long', k)
                                except:
                                    sll = []
                                    prt("ERROR STOP SELL")
                                if sll[0].get('orderId') != None:
                                    prt(str(sll[0].get('stopPrice')))

                                    # Записать время открытия позиции.
                                    check_file1 = os.path.exists(papka + "/" + "time_for_PNL.txt")  # True
                                    if check_file1 != True:
                                        recordFile(file="time_for_PNL", proffit_array=str(sll[0].get('updateTime')))

                                    long = 1
                                    sd = f"Long Пробитие уровня двумя свечами {k}"
                                    #recordFile(file='k', proffit_array=str(k))
                                    break
                                else:
                                    prt(str(sll))
                            l+=1
                            time.sleep(num_sec_sleep)
                else:
                    check_file1 = os.path.exists(papka + "/" + "pzd.txt")  # True
                    if check_file1 == True:
                        os.remove(papka + "/" + "pzd.txt")
                if long == 1 or signal == False:
                    break
                    ### Long ###
            if long == 1:
                # found a good enter point for Long
                prt(sd)
                signal = 'long'
                break
        time.sleep(1)
    return signal


# Telegram functions
telegram_delay = 25


def delate_files():
    # В начале цикла нужно удалить файл, если он существует
    check_file1 = os.path.exists(papka + "/" + "maxpositio.txt")  # True
    if check_file1 == True:
        os.remove(papka + "/" + "maxpositio.txt")

    check_file1 = os.path.exists(papka + "/" + "proffit_array.txt")  # True
    if check_file1 == True:
        os.remove(papka + "/" + "proffit_array.txt")

    check_file1 = os.path.exists(papka + "/" + "eth_profit.txt")  # True
    if check_file1 == True:
        os.remove(papka + "/" + "eth_profit.txt")

# Формируем массив профита и контрактов
def dvij_new(cen: float, okrug, a):
    n = 0  #
    stop = a  # Стоп шаг
    step_array = []
    one = cen - float(cen * temp_l)  # С какого пункта считать профит
    s = round((cen - one) / stop, okrug)  # Шаг пунктов монеты
    while True:
        if n > stop:
            break
        if n == stop:
            two = 0
        else:
            one += s
            two = random.randint(1, 1)
        step_array.append([round(one, okrug), two])
        n += 1
    return step_array



prt('Прошел час, я перезагрузился.')


def main(step):
    global proffit_array, maxpositio, eth_proffit_array, stop_percent

    try:
        try:
            process_messages()
        except:
            print('Error: Отказано в соединении')
        position = get_opened_positions(symbol, okrug)
        # print(position)
        open_sl = position[0]
        if open_sl == "":  # no position
            # В начале цикла нужно удалить файл, если он существует
            delate_files()

            # close all stop loss orders
            check_and_close_orders(symbol)

            loss_time = 0 # Проверка каждую секунду
            prt('Нет открытых позиций ' + str(symbol))

            check_file1 = os.path.exists(papka + "/" + "pzd.txt")  # True
            if check_file1 == True:
                signal = check_if_signal(step=1) # on
            else:
                signal = check_if_signal(step=0) # off

            if signal == 'long':

                # Открыть позицию
                down_limit = float(fileRead('maxpositio'))
                # down_limit = (limit_dep_stop) / (len(eth_proffit_array) - 1)
                # down_limit = round(down_limit, pos_okrug)
                # for j in range(0, len(eth_proffit_array) - 1):
                #     # print("down_limit -> ", down_limit)
                open_position_long(symbol, 'long', down_limit)

                # Включить повторное открытие.
                recordFile(file='pzd', proffit_array='1')

                maxpositio = maxposition(fileRead('dep_1000SHIB'))
                limit_dep_stop = float(maxpositio[0]) * float(maxpositio[1])
                if limit_dep_stop > 500 and limit_dep_stop <= 1500:
                    # Если позиция больше 500 $ - то позу набираем 5-ю частями
                    a = 7
                elif limit_dep_stop > 1500:
                    # Если позиция больше 1500 $ - то позу набираем 10-ю частями
                    a = 10
                else:
                    # Если позиция меньше 500 $ - то позу набираем 3-мя частями
                    a = 3

                cen = round(cena() * temp, okrug)
                eth_proffit_array = dvij_new(cen, okrug, a)
                proffit_array = copy.copy(eth_proffit_array)
                recordFile('eth_profit', str(proffit_array))
                recordFile('proffit_array', str(proffit_array))
                prt('Open position: ' + str(proffit_array))

        else:
            loss_time = 2 # Включается запуск скрипта каждые 3 - 7 сек.
            quantity = position[1]
            prt('Найдена открытая позиция ' + str(symbol) + ' ' + open_sl)
            prt('Кол-во: ' + str(quantity))
            prt('Баланс: ' + str(position[4]))

            proffit_array = fileRead('proffit_array')
            eth_proffit_array = fileRead('eth_profit')  # Для подсчета контрактов
            entry_price = position[5]  # enter price

            chek_price = client.futures_symbol_ticker(symbol=symbol)

            if position[0] == 'long':
                temp_arr = copy.copy(proffit_array)
                for j in range(0, len(temp_arr) - 1):
                    delta = temp_arr[j][0]
                    contracts = temp_arr[j][1]
                    if (float(chek_price['price']) > (entry_price + delta)):
                        # take profit
                        maxpositio = position[1]
                        if (len(proffit_array) < 3):
                            posAmt = client.futures_position_information(symbol=symbol)
                            if len(posAmt) > 0:
                                for oz in posAmt:
                                    if oz.get('positionSide') == 'LONG':
                                        pos_close = oz.get('positionAmt')
                                        # print(oz.get('positionAmt'))
                                        clo = close_position(symbol, 'long', float(pos_close), 1)
                                        if clo['positionSide'] == 'LONG':
                                            prt('Позиция закрыта полностью.')
                                            # Удалить файл с временем начала цикла.
                                            check_file1 = os.path.exists(
                                                papka + "/" + "time_for_PNL.txt")  # True
                                            if check_file1 == True:
                                                os.remove(papka + "/" + "time_for_PNL.txt")
                            else:
                                prt('Не могу закрыть позицию.')
                            break
                        else:
                            # print(maxpositio, contracts, chsti_contrakta)
                            kus = round(maxpositio * (contracts / summ_contracts(eth_proffit_array)), pos_okrug)
                            # print(kus)
                            close_position(symbol, 'long', abs(kus), 0)

                            del proffit_array[0]
                            recordFile('proffit_array', str(proffit_array))
                        prt(str(proffit_array))
        return loss_time

    except Exception as err:
        logging.error(err, exc_info=True)
        prt('nnSomething went wrong. Continuing...')


starttime = time.time()
timeout = time.time() + 59 * 60  # 60 seconds times 60 meaning the script will run for 365 days
counterr = 1

while time.time() <= timeout:
    try:
        if counterr < 2:
            prt("script continue running at " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        loss_time = main(step=timeout)
        counterr = counterr + 1
        if counterr > 5:
            counterr = 1
        if loss_time < 1:
            tr = random.randint(3, 7)
            # time.sleep(tr - ((time.time() - starttime) % int(tr)))  # 1 minute interval between each new execution
        else:
            tr = random.randint(3, 7)
            time.sleep(tr - ((time.time() - starttime) % int(tr)))  # 1 minute interval between each new execution
    except KeyboardInterrupt:
        print('nKeyboardInterrupt. Stopping.')
        exit()