## ReadMe Of Crawler

#### Design of table
- table name : trade_BTCUSDT_table

    |exch_name| Id | price | qty |quoteQty| time | isBuyerMaker |
    |----|-------|-----|-----------|-----------|---|---|
    
    - exch_name: name of exchange
    - Id: transaction id
    - price: the price in the transaction
    - qty: the amount of coins traded in the record
    - quoteQty: the amount of money in the record
    - time: the tick from 1970. Int
    - isBuyMaker: 
        - If the transaction order is provided by buyer : True
        - If the transaction order is provided by seller: False

    PRIMARY KEY(exch_name, ID)
