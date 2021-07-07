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