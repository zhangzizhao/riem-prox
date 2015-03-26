package riemann

import (
	"config"
	"errors"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"hash/fnv"
	"plog"
	"pool"
	"proto"
	"sync"
	"time"
)

type Msg struct {
	msg    *proto.Msg
	target int
	count  int
}

type Riemann struct {
	idx      int
	pool     *pool.Pool
	msgQueue chan Msg

	failedMsgs chan *proto.Msg
	count      int
	dead       bool
	deadLocal  bool
}

var riemann []Riemann
var allDead bool
var tryCount int
var zkConn *zk.Conn
var mu sync.Mutex

func init() {
	riemann = make([]Riemann, len(config.Conf.RiemannAddrs))
	allDead = false
	tryCount = 0

	var err error
	zkConn, _, err = zk.Connect(config.Conf.ZkAddrs, time.Second*10)
	if err != nil {
		fmt.Println("can not connect to zk, use local status")
	} else {
		tryCreatePath(config.Conf.ZkPath, zkConn)
	}

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
		riemann[idx].pool, err = pool.NewPool(config.Conf.NumInitConn, config.Conf.NumMaxConn, []string{addr})
		if err != nil {
			plog.Error("can not connect to riemann, addr: ", addr)
			riemann[idx].markDead()
		}
	}
	if zkConn != nil {
		snapshots, errors := watch(zkConn, config.Conf.ZkPath)
		go func() {
			for {
				select {
				case snapshot := <-snapshots:
					updateRiemannStatus(snapshot)
				case err := <-errors:
					plog.Error("watch zk failed: ", err)
				}
			}
		}()
	}
}

func (self *Riemann) forwardMsg() {
	for {
		msg := <-self.msgQueue
		plog.Info("try to forward msg, try times: ", msg.count)

		success := false
		if !self.dead {
			var err error
			success, err = self.innerSend(msg, 2)
			if !success && msg.count <= len(config.Conf.RiemannAddrs) {
				self.failedMsgs <- msg.msg
				plog.Warning("forward msg failed, err: ", err, "count: ", msg.count)
			}
		} else if self.deadLocal {
			if ok, err := self.innerSend(msg, 1); ok && err == nil {
				success = true
				plog.Info("reconnect to idx: ", self.idx)
				self.markAlive()
			}
		}
		if !success {
			if msg.count < 9999 {
				msg.count++
			}
			riemann[(self.idx+1)%len(riemann)].msgQueue <- msg
		}
	}
}

func (self *Riemann) innerSend(msg Msg, trynum int) (bool, error) {
	for try := 0; try < trynum; try += 1 {
		if self.pool == nil {
			var err error
			self.pool, err = pool.NewPool(config.Conf.NumInitConn, config.Conf.NumMaxConn, []string{config.Conf.RiemannAddrs[self.idx]})
			if err != nil {
				return false, errors.New(fmt.Sprintf("can not connect to riemann %d", self.idx))
			}
		}
		if conn, err := self.pool.Get(); err == nil {
			tcpTrans := NewTcpTransport(conn.Conn)
			if _, err := tcpTrans.SendRecv(msg.msg); err == nil {
				tcpTrans.Close()
				conn.Release()
				plog.Info("forward msg sucessfully, msg.target: ", msg.target, " idx: ", self.idx)
				return true, nil
			} else {
				conn.Close()
				if try == trynum-1 {
					self.pool.Close(conn)
					return false, err
				}
			}
		} else {
			return false, err
		}
	}
	return false, errors.New("send msg failed")
}

func chooseHost(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	i := h.Sum32() % uint32(len(config.Conf.RiemannAddrs))
	return int(i)
}

func Send(msg *proto.Msg) error {
	if allDead {
		mu.Lock()
		if tryCount > 100 {
			mu.Unlock()
			return errors.New("all riemanns are dead")
		}
		tryCount++
		mu.Unlock()
	}

	idx := chooseHost(*msg.Events[0].Service)
	riemann[idx].msgQueue <- Msg{msg, idx, 1}
	plog.Info("put msg into queue, idx: ", idx)
	return nil
}
