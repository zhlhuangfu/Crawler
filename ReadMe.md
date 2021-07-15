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
    - If None:
        continue

## Currently Parsed Exchanges
| Exchange | API | Request Limit | Running Interval | Remarks |
| -------- | --- | ------------- | ---------------- | ------- |
|[Huobi Global](https://huobiapi.github.io/docs/usdt_swap/v1/en/#general-query-a-batch-of-trade-records-of-a-contract)| https://api.hbdm.com/linear-swap-ex/market/history/trade?contract_code=BTC-USDT&size=100 | General 120 times request / 3 seconds for each IP| 60s | - 
|[Binance](https://binance-docs.github.io/apidocs/spot/en/#symbol-price-ticker)|https://api1.binance.com/api/v3/trades?symbol=BTCUSDT (1 weight for 1 coin)|1200 weights / min for each IP| 60s | -|
|[Coinbase](https://docs.pro.coinbase.com/#get-trades)|https://api.pro.coinbase.com/products/BTC-USDT/trades| 10 requests / second for each IP| 60s | - |
|[Kraken](https://docs.kraken.com/rest/)| https://api.kraken.com/0/public/Trades?pair=BTCUSDT&since=1625531618 | Every REST API user has a "call counter" which starts at 0, trade history calls increase the counter by 2. The maximum is 15, and the reduce rate is -0.33/second.| 60s | The limit is a problem. To solve it, one solutions is to crawl the transaction of different coins at cost of some some seconds delay| 
|[FTX](https://docs.ftx.com/#authentication) | https://ftx.com/api/markets/BTC/USDT/trades?start_time=1625531563&end_time=1625531657 | 30 requests / second | 60s | - |
|[Kucoin](https://docs.kucoin.top/#base-url) | https://api.kucoin.com/api/v1/market/histories?symbol=BTC-USDT | 1800 requests / minute | 30s | There seems no control of how many records or what time period of records returned by the endpoint.
| [Bithumb](https://apidocs.bithumb.com/) | https://api.bithumb.com/public/transaction_history/BTC_KRW |  135 requests / second | - |   As this exchanges only support transaction in Korean Won, not USD, so I ignoreed|
| [Bitfinex](https://docs.bitfinex.com/docs) |https://api-pub.bitfinex.com/v2/trades/tBTCUSD/hist (90 req/min) |rate limit is between 10 and 90 requests per minute, depending on the specific REST API endpoint| 60s | 
|[Gate.io](https://www.gate.io/docs/apiv4/en/index.html#retrieve-market-trades) | https://api.gateio.ws/api/v4/spot/trades?currency_pair=BTC_USDT&limit=1000 | 300 read operations per IP per second| 
|[biance.us](https://github.com/binance-us/binance-official-api-docs/blob/master/rest-api.md#general-info-on-limits) | https://api.binance.us/api/v3/trades?symbol=BTCUSDT (1 weight/request) | 1200 weight / minute |  
|[Bitstamp](https://www.bitstamp.net/api/#what-is-api) | https://www.bitstamp.net/api/v2/transactions/btcusdt/?time=hour | 8000 requests / 10 mins
|[Bitflyer](https://bitflyer.com/en-us/api) | https://api.bitflyer.com/v1/getexecutions?product_code=BTC_USD&count=100 | 500 queries per 5 minutes | - | The api seems not accessible.
|[Gemini](https://docs.gemini.com/rest-api/#two-factor-authentication) | https://api.gemini.com/v1/trades/btcusd | 120 requests per minute |
|[coinone](https://coinone.co.kr/exchange/trade/btc/krw) | - | - | -| It only contains KRW not USD, ignore|
|[Poloniex](https://docs.poloniex.com/#returntradehistory-public) | https://poloniex.com/public?command=returnTradeHistory&currencyPair=USDT_BTC&start=1610158341&end=1610499372 |  6 calls per second | 
|[Bitexen](http://docs.bitexen.com/)| https://www.bitexen.com/api/v1/order_book/BTCUSDT/ | 60 requests/min|
|[LBank](https://docs.lbkex.net/en/#url) | https://api.lbkex.com/v2/trades.do?symbol=btc_usdt&size=600 | 200 requests/10s |
|[ItBit](https://developer.paxos.com/docs/v2/api#tag/Orders) | https://api.paxos.com/v2/markets/BTCUSD/recent-executions
|[CoinEX](https://github.com/coinexcom/coinex_exchange_api/wiki/023deals) | https://api.coinex.com/v1/market/deals?market=BCHBTC| NA |
|[bitkub](https://www.bitkub.com/market/BTC)| - | - | - | It is a exchange for Thailand. Only sport THB, not USD or USDT|
|[bitbank](https://app.bitbank.cc/trade)| - | - | - | Only suppeort JPY |
|[CoinBit](https://www.coinbit.co.kr/)| - | - | - | Only KRW |
|[BitWell](https://bitwell-tech.github.io/oapi/#spot_matches) | https://openapi.bitwellex.com/pub/openapi/v1/hq/transaction?symbol=ETH_USDT | - | - | It only provised the latest transaction. We cannot have the recent transaction record.
|[CEX.io](https://cex.io/rest-api#trade-history) | https://cex.io/api/trade_history/BTC/USDT/ | 
|[Dex-Trade](https://docs.dex-trade.com/#api-Public_API-Trade_History) | https://api.dex-trade.com/v1/public/trades?pair=ETHUSDT | NA | 
|[gokumarket](https://dev.gokumarket.com/#/Trades/trades) | https://publicapi.gokumarket.com/trades?currency_pair=btc_usdt | 
|[EQONEX](https://developer.eqonex.com/#get-instrument-pairs) | https://eqonex.com/api/getTradeHistory?pairId={} | NA | 
| [AAX](https://www.aax.com/apidoc/index.html#rate-limits)| https://api.aax.com/v2/market/trades?symbol=BTCUSDT&limit=10 |10 / s, 150/min, 5000/h|
|[HitBTC](https://api.hitbtc.com/#rate-limiting) | https://api.hitbtc.com/api/2/public/trades/ETHBTC?sort=DESC | 10 request / s|


