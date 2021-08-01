#!/bin/bash
process_name="crawl_trades_from_exchs.py"

# 添加启动命令
function start(){
    echo "start..."
    exchs=(binance binance.us bitfinex bitflyer bitstamp coinbase ftx gate.io gemini huobi kraken kucoin poloniex 
            ascendEX bittrex coinDCX cryptoComExchange ftxUS huobiKorea liquid OKEx proBitGlobal
            Lbank ItBit CoinEX CEXIO Dex GokuMarket EQONext AAX HitBTC
            BigOne Indodax Okcoin EXMO BithumbGlobal  BtcTurkPro Wootrade Bitfront ZBcom CoinlistPro)
    for name in ${exchs[*]}; 
    do 
        log_name="logs/crawl_from_${name}.log"
        echo ${log_name}
        nohup python -u $process_name --exch_name $name > $log_name 2>&1 &
    done;
    echo "start successful"
    return 0
}

# 添加停止命令
function stop(){
    echo "stop..."

    seq=$(ps aux |grep $process_name |grep -v grep| awk '{print $2}')
    for ss in $seq;
    do
        echo "kill -9 ${ss}"
        kill -9 $ss
    done;

    echo "stop successful"
    return 0
}

case $1 in
"start")
    start
    ;;
"stop")
    stop
    ;;
*)
    echo "请输入: start, stop"
    ;;
esac