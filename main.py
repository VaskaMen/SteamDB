import asyncio
import sqlite3
import time
import requests
from datetime import  datetime
from colorama import Fore, Back, Style
import threading

con = sqlite3.connect("SteamPrice.db")
cursor = con.cursor()

cursor.execute(
        """CREATE TABLE IF NOT EXISTS apps 
        (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        app_id INTEGER PRIMARY KEY,
        app_name TEXT
        )
        """)

def add_game():
    all_games_req = requests.get(f"http://api.steampowered.com/ISteamApps/GetAppList/v0002/")
    all_games_req = all_games_req.json()
    allGames = all_games_req['applist']['apps']
    for a in allGames:
        game = (a['appid'], f"{a['name']}")
        cursor.execute(f"INSERT OR IGNORE INTO apps (app_id, app_name) VALUES (?, ?)", game)
    con.commit()


def create_price_table(country):
    cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS price_{country}
        (
        app_id INTEGER,
        initial INTEGER,
        initial_formatted VARCHAR(50),
        discount_percent INTEGER,
        final INTEGER,
        final_formatted VARCHAR(50),
        date DATE
        )
        """
    )

def priceRub(countru):
    create_price_table(countru)
    all_games_req = requests.get(f"http://api.steampowered.com/ISteamApps/GetAppList/v0002/").json()
    allGames = all_games_req['applist']['apps']
    step = 900

    apps = []
    for a in allGames:
        apps.append(a['appid'])


    for i in range(0, len(apps), step):

        if i + step > len(apps):
            end = len(apps) - 1
        else:
            end = i + step


        pros = round((i/len(apps)) * 100, 3)


        print(f"{'{:.3f}'.format(pros)}% [{'▬' * int((pros/5))}{'•' * int(((100 - pros)/5))}] {countru}")

        price_list = requests.get(
            f"http://store.steampowered.com/api/appdetails/?filters=price_overview&"
            f"appids={','.join(map(str, apps[i:end] ))}&cc={countru}")

        # Заполнение таблицы
        if price_list.status_code == 200:
            price_list = price_list.json()
            for game in price_list:
                if game in price_list and price_list[f'{game}']['success'] and price_list[f'{game}']['data'] != []:
                    price_overview = price_list[f'{game}']['data']['price_overview']
                    if diferent_price(game, countru, price_overview):
                        add_game(countru,
                                 game,
                                 price_overview['initial'],
                                 price_overview['initial_formatted'],
                                 price_overview['discount_percent'],
                                 price_overview['final'],
                                 price_overview['final_formatted']
                                 )
                else:
                    price_overview = {
                            'initial': 0,
                            'discount_percent': 0,
                            'final': 0
                    }
                    if diferent_price(game, countru, price_overview):
                        add_game(countru, game)

        else:
            print("#############################ERROR")
            time.sleep(20)


def add_game(country, app_id, initial=0, initial_formatted='', discount_percent=0, final=0, final_formatted=''):
    cursor.execute(f"""INSERT INTO price_{country} VALUES (
                                               {app_id},
                                               {initial},
                                               '{initial_formatted}',
                                               {discount_percent},
                                               {final},
                                               '{final_formatted}',
                                               '{datetime.today().strftime("%d.%m.%Y")}'
                                               )""")
    con.commit()

def last_price(app_id, country):
    cursor.execute(f"SELECT * FROM price_{country} WHERE app_id = {app_id} ORDER BY date DESC LIMIT 1")
    result = cursor.fetchall()
    if result != []:
        result = result[0]
        return {
                'country': country,
                'app_id': result[0],
                'initial': result[1],
                'initial_formatted': result[2],
                'discount_percent': result[3],
                'final': result[4],
                'final_formatted': result[5],
                'date': result[6]
            }

def diferent_price(app_id, country, price_overview):
    game = last_price(app_id, country)
    if game != None:
        initial = game['initial'] != price_overview['initial']
        discount_percent = game['discount_percent'] != price_overview['discount_percent']
        final = game['final'] != price_overview['final']
        if initial and discount_percent and final:
            return True
        else: return False
    else:
        return False

coun = ["US", "RU", "TR", "KZ"]

for i in coun:
    t = time.time()
    priceRub(i)
    print(time.time() - t)
