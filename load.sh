#!/bin/bash

cd `dirname $0` || exit
#ulimit -c unlimited
mkdir -p status/riemann-proxy

start(){
    stop
    sleep 1
    setsid ./bin/supervise.riemann-proxy -u status/riemann-proxy ./bin/riemann-proxy -config=conf/proxy.yaml </dev/null &>/dev/null &
}

stop(){
    killall -9 supervise.riemann-proxy riemann-proxy
}

restart(){
    killall riemann-proxy
}

case C"$1" in
    Cstart)
        start
        echo "start Done!"
        ;;
    Cstop)
        stop
        echo "stop Done!"
        ;;
    Crestart)
        restart
        echo "restart Done!"
        ;;
    C*)
        echo "Usage: $0 {start|stop|restart}"
        ;;
esac
