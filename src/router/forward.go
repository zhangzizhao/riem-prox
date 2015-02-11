package router

import (
    "riemann"
    "config"
    "net"
    "hash/fnv"
    pool "gopkg.in/fatih/pool.v2"
    "plog"
)

type Msg struct {
    msg *riemann.Msg
    target int
    count int
}
type Forwarder struct {
    pool pool.Pool
    msgQueue chan Msg
    idx int
}

var forwarders []Forwarder

func init() {
    forwarders = make([]Forwarder, len(config.Conf.RiemannAddrs))
    for idx, addr := range config.Conf.RiemannAddrs {
        addr1 := addr//a very strange bug...
        factory := func() (net.Conn, error) { return net.Dial("tcp", addr1)}
        var err error
        forwarders[idx].pool, err = pool.NewChannelPool(10, 1000, factory)
        forwarders[idx].idx = idx
        if err != nil {
            plog.Error("can not connect to riemann, addr: ", addr)
            config.MarkDead(idx)
        }
        forwarders[idx].msgQueue = make(chan Msg, 5000)
        go forwarders[idx].run()
    }
}

func (f *Forwarder) run() {
    for {
        msg := <- f.msgQueue
        if msg.count >= 4 || (f.idx == msg.target && msg.count > 1) {
            plog.Error("forward msg failed, msg.target: ", msg.target, " msg.count: ", msg.count, " idx:", f.idx)
            continue
        }
        msg.count += 1
        if config.Conf.DeadRiemann[f.idx] {
            forwarders[(f.idx+1)%len(forwarders)].msgQueue <- msg
            if msg.target == f.idx && config.Conf.DeadLocal[f.idx] {
                factory := func() (net.Conn, error) { return net.Dial("tcp", config.Conf.RiemannAddrs[f.idx])}
                var err error
                forwarders[f.idx].pool, err = pool.NewChannelPool(10, 1000, factory)
                if err == nil {
                    if conn, err := f.pool.Get(); err == nil {
                        tcp := NewTcpTransport(conn)
                        if _, err := tcp.SendRecv(msg.msg); err == nil {
                            plog.Info("reconnect to idx: ", f.idx)
                            config.MarkAlive(f.idx)
                        }
                    }
                }
            }
        } else {
            success := false
            for try:=0; try<2 && !success; try+=1 {
                if conn, err := f.pool.Get(); err == nil {
                    tcp := NewTcpTransport(conn)
                    if _, err := tcp.SendRecv(msg.msg); err == nil {
                        success = true
                        plog.Info("forward msg sucessfully, msg.target: ", msg.target, " idx: ", f.idx, " try: ", try)
                    } else {
                        plog.Warning("forward msg failed, err: ", err)
                    }
                } else {
                    plog.Error("get conn from pool failed, err: ", err, " idx: ", f.idx)
                }
            }
            if !success {
                forwarders[(f.idx+1)%len(forwarders)].msgQueue <- msg
                config.MarkDead(f.idx)
            }
        }
    }
}

func chooseHost(s string) int {
    h := fnv.New32a()
    h.Write([]byte(s))
    i := h.Sum32()%uint32(len(config.Conf.RiemannAddrs))
    return int(i)
}

func Send(msg *riemann.Msg) {
    idx := chooseHost(*msg.Events[0].Service)
    forwarders[idx].msgQueue <- Msg{msg, idx, 1}
}