package pool

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

type Pool struct {
	addrs []string
	conns []chan net.Conn
	mu    sync.Mutex
}

func NewPool(initNum, maxCap int, addrs []string) (*Pool, error) {
	if initNum < 0 || maxCap <= 0 || initNum > maxCap {
		return nil, errors.New("invalid capacity settings")
	}
	p := &Pool{addrs: addrs, conns: make([]chan net.Conn, len(addrs))}
	for i := 0; i < len(addrs); i++ {
		p.conns[i] = make(chan net.Conn, maxCap)
		for j := 0; j < initNum; j++ {
			if conn, err := net.Dial("tcp", addrs[i]); err != nil {
				return nil, errors.New(fmt.Sprintf("fail to connect to %s", addrs[i]))
			} else {
				p.conns[i] <- conn
			}
		}
	}
	return p, nil
}

func (self *Pool) Get() (poolConn, error) {
	self.mu.Lock()
	defer self.mu.Unlock()
	rand.Seed(int64(time.Now().Nanosecond()))
	idx := rand.Intn(len(self.addrs))
	var errString string
	for i := 0; i < len(self.addrs); i++ {
		select {
		case conn := <-self.conns[idx]:
			return poolConn{conn, idx, self}, nil
		default:
			if conn, err := net.Dial("tcp", self.addrs[idx]); err == nil {
				return poolConn{conn, idx, self}, nil
			} else {
				errString = err.Error()
			}
		}
		idx = (idx + 1) % len(self.addrs)
	}
	return poolConn{}, errors.New(errString)
}

func (self *Pool) put(conn *poolConn) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if conn == nil {
		return
	}
	select {
	case self.conns[conn.idx] <- conn.Conn:
		return
	default:
		conn.Close()
	}
}

func (self *Pool) Close(conn poolConn) {
	self.mu.Lock()
	defer self.mu.Unlock()
	idx := conn.idx
	for {
		select {
		case conn := <-self.conns[idx]:
			conn.Close()
		default:
			return
		}
	}
}
