#!/bin/bash
process_name="crawl_real_time_trades.py"

# 添加启动命令
function start(){
    echo "start..."
    ks=$(seq 0 1 124)
    for k in ${ks[*]};
    do 
        log_name="logs/real_time/real_time_${k}.log"
        echo ${log_name}
        nohup python -u $process_name --kth $k > $log_name 2>&1 &
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