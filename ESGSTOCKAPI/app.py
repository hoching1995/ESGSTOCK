from flask import Flask
from flask import request
import requests
import pyodbc
import simplejson as json
import re
from bs4 import BeautifulSoup
import requests
from tiingo import TiingoClient

app = Flask(__name__)

config = {}

headers = {
    'Content-Type': 'application/json'
}
requestResponse = requests.get("https://api.tiingo.com/api/test?token=9ebbb489c64efba75e8f93afc774f5738f89dd20",
                               headers=headers)
print(requestResponse.json())
config['session'] = True
config['api_key'] = "9ebbb489c64efba75e8f93afc774f5738f89dd20"
client = TiingoClient(config);


ticker_price = client.get_ticker_price("GOOGL", frequency="daily")
# print(ticker_price)
# historical_prices = client.get_ticker_price("GOOGL",
#                                             fmt='json',
#                                             startDate='2017-08-01',
#                                             endDate='2017-08-31',
#                                             frequency='daily')
# ticker_history = client.get_ticker_metadata("GOOGL")

# print(ticker_price)
@app.route("/meta")
def metaData():
    input = request.args.get('q')
    data = client.get_ticker_metadata(input)
    return json.dumps(data, use_decimal=True)


@app.route("/stockprice")
def stockPrice():
    input = request.args.get('q')

    # check DB for recent stock data
    # stockData = getStockData(db, input)
    # if (stockData)
    # return json(stockData)

    ticker_price = client.get_ticker_price(input, frequency="daily")
    print(ticker_price)
    # storePrice(db, input, ticker_price)
    return json.dumps(ticker_price, use_decimal=True)


@app.route("/")
def hello():
    db = connectdatabase()
    return "Hello, World!"


@app.route("/top-ten")
def topTen():
    db = connectdatabase()
    return json.dumps(fetchtopten(db), use_decimal=True)


@app.route("/search")
def liveQuary():
    db = connectdatabase()
    input = request.args.get('q')
    return json.dumps(fetchInput(db, input), use_decimal=True)


@app.route("/hotstock")
def hotStock():
    db = connectdatabase()
    return json.dumps(fetchHotStock(db), use_decimal=True)


@app.route("/detials")  # /details
def totalnumberindustryquary():
    db = connectdatabase()
    industry = request.args.get('industry')
    company = request.args.get('company')

    print(industry)
    print(company)
    list = json.dumps({
        "industryTotal": fetchtotalnumberindustry(db, industry),
        "industryRank": fetchIndustryRank(db, company, industry),
        "globalRank": fetchGlobalRank(db, company),
        "globalTotal": fetchglobalTotalRank(db),
    })
    print(list)
    return json.dumps({
        "industryTotal": fetchtotalnumberindustry(db, industry),
        "industryRank": fetchIndustryRank(db, company, industry),
        "globalRank": fetchGlobalRank(db, company),
        "globalTotal": fetchglobalTotalRank(db),
    })


def fetchGlobalRank(cnxn, company):
    cursor = cnxn.cursor()
    query = "select RANK from (SELECT ROW_NUMBER() OVER(ORDER BY [esgrating] ASC)AS Rank, [name] FROM ESGSTOCK) at where [name] = '{}'".format(
        company)
    print(query)
    cursor.execute(query)
    row = cursor.fetchone()
    return row.RANK


def fetchHotStock(cnxn):
    cursor = cnxn.cursor()
    stocks = []
    query = ["SELECT * FROM ESGSTOCK Where [name] like 'Amazon.com Inc'",
             "SELECT * FROM ESGSTOCK Where [name] like 'Facebook Inc'",
             "SELECT * FROM ESGSTOCK Where [name] like 'NVIDIA Corp.'",
             "SELECT * FROM ESGSTOCK Where [name] like 'QUALCOMM, Inc.'",
             "SELECT * FROM ESGSTOCK Where [name] like 'Bank of America Corp.'",
             "SELECT * FROM ESGSTOCK Where [name] like 'Beyond Meat, Inc.'",
             "SELECT * FROM ESGSTOCK Where [name] like 'Microsoft Corp'",
             "SELECT * FROM ESGSTOCK Where [name] like 'Advanced Micro Devices Inc'",
             "SELECT * FROM ESGSTOCK Where [name] like 'Tesla Inc'",
             "SELECT * FROM ESGSTOCK Where [name] like 'Alphabet Inc'",
             ]

    for queries in query:
        cursor.execute(queries)
        columns = [column[0] for column in cursor.description]
        for row in cursor:
            print(row)
            data = dict(zip(columns, row))
            stocks.append(data)
    return stocks


def fetchglobalTotalRank(cnxn):
    cursor = cnxn.cursor()
    query = "SELECT COUNT([name]) AS totalrow from ESGSTOCK"
    cursor.execute(query)
    row = cursor.fetchone()
    return row.totalrow


def fetchIndustryRank(cnxn, company, industry):
    cursor = cnxn.cursor()
    query = "select RANK from (SELECT ROW_NUMBER() OVER(ORDER BY [esgrating] ASC)AS Rank, [name] FROM ESGSTOCK where [group] = '{}') as ranked where ranked.name = '{}'".format(
        industry, company)
    cursor.execute(query)
    row = cursor.fetchone()
    return row.RANK


def fetchInput(cnxn, input):
    cursor = cnxn.cursor()
    # query = "SELECT * FROM ESGSTOCK Where [name] LIKE '{}%' OR ticker = '{}'".format(input, input)
    query = "SELECT t.*, e.* FROM tickertable t FULL OUTER JOIN ESGSTOCK e ON t.stockticker = e.ticker Where e.ticker Like '{}' or t.company Like '{}%' or t.stockticker Like '{}%' or e.name Like'{}%' ".format(input, input, input, input)
    print(query)
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    stocks = []
    for row in cursor:
        print(row)
        data = dict(zip(columns, row))
        stocks.append(data)
    return stocks


def connectdatabase():
    # Some other example server values are
    # server = 'localhost\sqlexpress' # for a named instance
    # server = 'myserver,port' # to specify an alternate port
    # server = 'tcp:esgstockdata.database.windows.net'
    server = 'tcp:newesgstockdata.database.windows.net'
    # newesgstockdata
    # database = 'esgstockdata'
    database = 'newesgstockdata'
    username = 'esgstockadmin'
    password = 'passwordAs!'
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

    return cnxn


def fetchtopten(cnxn):
    cursor = cnxn.cursor()
    cursor.execute("SELECT TOP(10)* FROM ESGSTOCK Where [group] = 'Automobiles' ORDER by esgrating ASC ")

    columns = [column[0] for column in cursor.description]
    stocks = []
    for row in cursor:
        print(row)
        data = dict(zip(columns, row))
        stocks.append(data)
    return stocks


def fetchtotalnumberindustry(cnxn, industry):
    cursor = cnxn.cursor()
    query = "SELECT COUNT([name]) as industrytotalnumber from ESGSTOCK Where [group] LIKE '{}'".format(industry)
    cursor.execute(query)
    row = cursor.fetchone()
    print(row)
    return row.industrytotalnumber

# @app.route("/get-news")
# def getnews():
# url = 'https://www.google.com/search?q=esg+score+stocks&tbm=nws'
# page = requests.get(url)
# soup = BeautifulSoup(page.content, 'html.parser')
# gCards = soup.find_all("g-card")
#
# html = []
# for cards in gCards:
#     html.append(cards.text)
#
# print(page.content)

# return 'no news yet'
