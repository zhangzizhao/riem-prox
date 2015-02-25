package pool

import (
	"net"
)

type poolConn struct {
	Conn net.Conn
	idx  int
	pool *Pool
}

func (self *poolConn) Release() {
	self.pool.put(self)
}

func (self *poolConn) Close() {
	self.Conn.Close()
}
