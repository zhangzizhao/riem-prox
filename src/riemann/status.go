package riemann

import (
	"config"
	"github.com/samuel/go-zookeeper/zk"
	"plog"
	"strconv"
	"strings"
	"time"
)

func (self *Riemann) updateStatus() {
	for {
		select {
		case <-self.failedMsgs:
			self.count += 1
			if self.count > 5 {
				self.markDead()
				self.count = 0
			}
		case <-time.After(time.Minute):
			self.count = 0
		}
	}
}

func (self *Riemann) markDead() {
	if self.deadLocal {
		return
	}
	self.dead = true
	self.deadLocal = true
	conn, _, err := zk.Connect(config.Conf.ZkAddrs, time.Second*10)
	defer conn.Close()
	if err == nil {
		conn.Create(config.Conf.ZkPath+"/"+config.Conf.LocalIP+"-"+strconv.Itoa(self.idx), []byte(""), int32(0), zk.WorldACL(zk.PermAll))
	}
}

func (self *Riemann) markAlive() {
	if !self.deadLocal {
		return
	}
	self.dead = false
	self.deadLocal = false
	conn, _, err := zk.Connect(config.Conf.ZkAddrs, time.Second*10)
	defer conn.Close()
	if err == nil {
		conn.Delete(config.Conf.ZkPath+"/"+config.Conf.LocalIP+"-"+strconv.Itoa(self.idx), -1)
	}
}

func watch(conn *zk.Conn, path string) (chan []string, chan error) {
	snapshots := make(chan []string)
	errors := make(chan error)
	go func() {
		for {
			snapshot, _, events, err := conn.ChildrenW(path)
			if err != nil {
				errors <- err
				return
			}
			snapshots <- snapshot
			evt := <-events
			if evt.Err != nil {
				errors <- evt.Err
				return
			}
		}
	}()
	return snapshots, errors
}

func tryCreatePath(path string, conn *zk.Conn) {
	if exists, _, err := conn.Exists(path); err == nil && exists {
		if children, _, err := conn.Children(path); err == nil {
			updateRiemannStatus(children)
		}
		return
	}
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)
	for i := 1; i <= len(path); i++ {
		if i == len(path) || path[i] == '/' {
			if exists, _, err := conn.Exists(path[:i]); err == nil && !exists {
				conn.Create(path[:i], []byte(""), flags, acl)
			}
		}
	}

}
func updateRiemannStatus(children []string) {
	dead := make([]bool, len(riemann))
	local := make([]bool, len(riemann))
	for _, val := range children {
		s := strings.Split(val, "-")
		if idx, err := strconv.Atoi(s[len(s)-1]); err == nil {
			dead[idx] = true
			if s[0] == config.Conf.LocalIP {
				local[idx] = true
			}
		}
	}
	aliveCount := 0
	for i, val := range dead {
		riemann[i].dead = val
		riemann[i].deadLocal = local[i]
		if !val {
			aliveCount += 1
		}
	}
	if aliveCount == 0 {
		panic("all riemann dead")
	}
	plog.Info("riemann status updated, dead riemanns: ", dead)
}
