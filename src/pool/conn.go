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

func (self *poolConn) Read(b []byte) (int, error) {
	return self.Conn.Read(b)
}

func (self *poolConn) Write(b []byte) (int, error) {
	return self.Conn.Write(b)
}
