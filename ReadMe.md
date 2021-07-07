## ReadMe Of Crawler

#### Design of table
- table name : trade_BTCUSDT_table

    |exch_name| Id | price | qty |quoteQty| time | isBuyerMaker |
    |----|-------|-----|-----------|-----------|---|---|
    
    - exch_name: name of exchange VARCHAR(255)
    - Id: transaction id VARCHAR(255)
    - price: the price in the transaction DOUBLE
    - qty: the amount of coins traded in the record DOUBLE
    - quoteQty: the amount of money in the record DOUBLE
    - time: the tick from 1970. BITINT(8)
    - isBuyMaker: 
        - If the transaction order is provided by buyer : True
        - If the transaction order is provided by seller: False

    PRIMARY KEY(exch_name, ID)

- Notes:
    - Mysql Shell:
        ```bash
        mysql -u root -p
        ```
        passwd:123456

    - Pay attention to the timestamp
        - 1625465785723 in ms (not preferred)
        - 1625465785 in s



    |exch_name| API | Request Limit | Running interval |Remarks|
    |----|-------|-----|-----------|-----------|
    |Bittrex|https://api.bittrex.com/v3/markets/{BTC-USDT}/trades||60|have no market of BNBUSDT|
    |OKEx|https://www.okex.com/api/spot/v3/instruments/{BTC-USDT}/trades?limit=100|20 times/2 s for get trades api|60|have no market of BNBUSDT|
    |Liquid|get trades: https://api.liquid.com/executions?product_id={}&timestamp={}&limit=100 <br> get product_id: https://api.liquid.com/products|300 requets per 5 minutes|60|only have markets of "BTCUSDT", "ETHUSDT", "DOTUSDT", "UNIUSDT"|
    |Crypto.com Exchange|https://api.crypto.com/v2/public/get-trades?instrument_name={BTC_USDT}|100 requests per second|60|have no market of BNBUSDT|
    |Huobi Korea|https://api-cloud.huobi.co.kr/market/history/trade?symbol={}&size=500|10 requests per second|60|have no market of BNBUSDT|
    |ProBit Global|https://api.probit.com/api/exchange/v1/trade?market_id={}&start_time={}&end_time={}&limit=1000|20 requests per second|60|-|
    |AcendEX|https://ascendex.com/api/pro/v1/trades?symbol={BTC/USDT}&n=50||60||
    |CoinDCX|https://public.coindcx.com/market_data/trade_history?pair={B-BTC_USDT}&limit=50||60||
    |FTX US|https://ftx.us/api/markets/{}/trades?start_time={}&end_time={}|30 requests per second|60|have no market of BNBUSDT, ADAUSDT, DOTUSDT|
    |CoinCheck|---|---|---|Only supports jpy|
    |Zaif|---|---|---|Only supports jpy|
    |Upbit|---|---|---|Only supports krw|
    |Korbit|---|---|---|not sure if it has api for trades|