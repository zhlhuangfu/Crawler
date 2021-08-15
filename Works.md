## Crawling real-time data from exchanges:
- Related scripts:
    - cmd_real_time.sh
    - crawl_real_time_trades.py
- Currently it crawl transactions of top 500 cryptocoins from top 15 exchanges. 
    - TODO: Add two more exchanges that has licences
- Proxies:
    - To handle the issue of IP limitation, for some exchanges, proxies are used.
    - [Storm Proxy](https://stormproxies.com/)
        - username: on slack
        - password: on slack
- The data are stored in mysql
    - Database name is : trade_info.
    - Table name: Eg. tade_of_BTCUSDT

## Prices from coinmarketcap
- Background:
    - we subscribe one month for use of API from CoinMarketCap(CMC) to get historical prices of cryptocoins.
- The data are stroed in mysql
    - Database name: crypto_coin
    - Table name: Eg. quote_info_of_{symbol_id}
- Symbol id:
    - The id is an unique id provided by CMC
- APIs from CMC:
    - CMC provide endpoints to get symbol ids and some other informations. Please refer to CMC website.
    - U can also refer to scripts: cluster.py to take an example.
    
## Ranking Coins
- Background
    - We want to analysis the correlation between coins and see which coins has important influential on others.
- Paper:Ranking influential and influenced Shares
    - Topics:
    - Ranking algoritms: PageRank, HITS
    - Transfer Entropy
- Related Scripts:On github Mars-1, branch: haoli/CoinPrice
    - PageRank & HITS: Modules/rank_alg.py
    - Transfer Entropy: Modules/time_series.py, parallel_te.py (parallel version)