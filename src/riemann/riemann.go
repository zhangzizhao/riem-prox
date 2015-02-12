package riemann

import (
	"config"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	pool "gopkg.in/fatih/pool.v2"
	"hash/fnv"
	"net"
	"plog"
	"proto"
	"time"
)


type Msg struct {
	msg    *proto.Msg
	target int
	count  int
	used   int
}

type Riemann struct {
	idx      int
	pool     pool.Pool
	msgQueue chan Msg

	failedMsgs chan *proto.Msg
	count      int
	dead       bool
	deadLocal  bool
}

var riemann []Riemann

func init() {
	riemann = make([]Riemann, len(config.Conf.RiemannAddrs))
	for idx, addr := range config.Conf.RiemannAddrs {
		riemann[idx].idx = idx

		riemann[idx].msgQueue = make(chan Msg, 5000)
		go riemann[idx].forwardMsg()

		riemann[idx].failedMsgs = make(chan *proto.Msg, 1000)
		riemann[idx].count = 0
		riemann[idx].dead = false
		riemann[idx].deadLocal = false
		go riemann[idx].updateStatus()

		var err error
		addr1 := addr //a very strange bug...
		factory := func() (net.Conn, error) { return net.Dial("tcp", addr1) }
		riemann[idx].pool, err = pool.NewChannelPool(config.Conf.NumInitConn, config.Conf.NumMaxConn, factory)
		if err != nil {
			plog.Error("can not connect to riemann, addr: ", addr)
			riemann[idx].markDead()
		}
	}

	conn, _, err := zk.Connect(config.Conf.ZkAddrs, time.Second*10)
	if err != nil {
		fmt.Println("can not connect to zk, use local status")
	}

	tryCreatePath(config.Conf.ZkPath, conn)

	snapshots, errors := watch(conn, config.Conf.ZkPath)
	go func() {
		for {
			select {
			case snapshot := <-snapshots:
				updateRiemannStatus(snapshot)
			case err := <-errors:
				panic(err)
			}
		}
	}()
}

func (self *Riemann) forwardMsg() {
	for {
		msg := <-self.msgQueue
		if msg.count > len(config.Conf.RiemannAddrs) {
			plog.Error("forward msg failed, msg.target: ", msg.target, " msg.count: ", msg.count, " idx:", self.idx)
			continue
		}
		msg.count += 1

		success := false
		if !self.dead && (msg.used&(1<<uint(self.idx))) == 0 {
			var err error
			success, err = self.innerSend(msg, 2)
			if !success {
				self.failedMsgs <- msg.msg
				plog.Warning("forward msg failed, err: ", err)
			}
			msg.used |= (1 << uint(self.idx))
		}
		if msg.target == self.idx && self.deadLocal {
			factory := func() (net.Conn, error) { return net.Dial("tcp", config.Conf.RiemannAddrs[self.idx]) }
			var err error
			self.pool, err = pool.NewChannelPool(config.Conf.NumInitConn, config.Conf.NumMaxConn, factory)
			if err == nil {
				if ok, err := self.innerSend(msg, 1); ok && err == nil {
					plog.Info("reconnect to idx: ", self.idx)
					self.markAlive()
				}
			}
		}
		if !success {
			riemann[(self.idx+1)%len(riemann)].msgQueue <- msg
		}
	}
}

func (self *Riemann) innerSend(msg Msg, trynum int) (bool, error) {
	for try := 0; try < trynum; try += 1 {
		if conn, err := self.pool.Get(); err == nil {
			tcp := NewTcpTransport(conn)
			if _, err := tcp.SendRecv(msg.msg); err == nil {
				plog.Info("forward msg sucessfully, msg.target: ", msg.target, " idx: ", self.idx, " try: ", try)
				return true, nil
			} else if try == trynum-1 {
				return false, err
			}
			conn.Close()
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

func Send(msg *proto.Msg) {
	idx := chooseHost(*msg.Events[0].Service)
	riemann[idx].msgQueue <- Msg{msg, idx, 1, 0}
}
