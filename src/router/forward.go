package router

import (
	"config"
	pool "gopkg.in/fatih/pool.v2"
	"hash/fnv"
	"net"
	"plog"
	"riemann"
	"rstatus"
)

type Msg struct {
	msg    *riemann.Msg
	target int
	count  int
	used   int
}
type Forwarder struct {
	pool     pool.Pool
	msgQueue chan Msg
	idx      int
}

var forwarders []Forwarder

func init() {
	forwarders = make([]Forwarder, len(config.Conf.RiemannAddrs))
	for idx, addr := range config.Conf.RiemannAddrs {
		addr1 := addr //a very strange bug...
		factory := func() (net.Conn, error) { return net.Dial("tcp", addr1) }
		var err error
		forwarders[idx].pool, err = pool.NewChannelPool(10, 1000, factory)
		forwarders[idx].idx = idx
		if err != nil {
			plog.Error("can not connect to riemann, addr: ", addr)
			rstatus.Statuses[idx].MarkDead()
		}
		forwarders[idx].msgQueue = make(chan Msg, 5000)
		go forwarders[idx].run()
	}
}

func (f *Forwarder) run() {
	for {
		msg := <-f.msgQueue
		if msg.count > len(config.Conf.RiemannAddrs) {
			plog.Error("forward msg failed, msg.target: ", msg.target, " msg.count: ", msg.count, " idx:", f.idx)
			continue
		}
		msg.count += 1

		success := false
		if !rstatus.Statuses[f.idx].DeadRiemann && (msg.used&(1<<uint(f.idx))) == 0 {
			var err error
			success, err = f.innerSend(msg, 2)
			if !success {
				rstatus.Statuses[f.idx].Msgs <- msg.msg
				plog.Warning("forward msg failed, err: ", err)
			}
			msg.used |= (1 << uint(f.idx))
		} else if msg.target == f.idx && rstatus.Statuses[f.idx].DeadLocal {
			factory := func() (net.Conn, error) { return net.Dial("tcp", config.Conf.RiemannAddrs[f.idx]) }
			var err error
			forwarders[f.idx].pool, err = pool.NewChannelPool(10, 1000, factory)
			if err == nil {
				if ok, err := f.innerSend(msg, 1); ok && err == nil {
					plog.Info("reconnect to idx: ", f.idx)
					rstatus.Statuses[f.idx].MarkAlive()
				}
			}
		}
		if !success {
			forwarders[(f.idx+1)%len(forwarders)].msgQueue <- msg
		}
	}
}

func (f *Forwarder) innerSend(msg Msg, trynum int) (bool, error) {
	for try := 0; try < trynum; try += 1 {
		if conn, err := f.pool.Get(); err == nil {
			tcp := NewTcpTransport(conn)
			if _, err := tcp.SendRecv(msg.msg); err == nil {
				plog.Info("forward msg sucessfully, msg.target: ", msg.target, " idx: ", f.idx, " try: ", try)
				return true, nil
			} else if try == trynum-1 {
				return false, err
			}
		}
	}
	return false, nil
}

func chooseHost(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	i := h.Sum32() % uint32(len(config.Conf.RiemannAddrs))
	return int(i)
}

func Send(msg *riemann.Msg) {
	idx := chooseHost(*msg.Events[0].Service)
	forwarders[idx].msgQueue <- Msg{msg, idx, 1, 0}
}
