package riemann

import (
	"io"
	"net"
	"proto"
	"utils"
)

type TcpTransport struct {
	conn         net.Conn
	requestQueue chan request
}

type request struct {
	message     *proto.Msg
	response_ch chan response
}

type response struct {
	message *proto.Msg
	err     error
}

func NewTcpTransport(conn net.Conn) *TcpTransport {
	t := &TcpTransport{
		conn:         conn,
		requestQueue: make(chan request),
	}
	go t.runRequestQueue()
	return t
}

func (t *TcpTransport) SendRecv(message *proto.Msg) (*proto.Msg, error) {
	response_ch := make(chan response)
	t.requestQueue <- request{message, response_ch}
	r := <-response_ch
	return r.message, r.err
}

func (t *TcpTransport) Close() error {
	close(t.requestQueue)
	/*err := t.conn.Close()
	if err != nil {
		return err
	}*/
	return nil
}

func (t *TcpTransport) runRequestQueue() {
	for req := range t.requestQueue {
		message := req.message
		response_ch := req.response_ch

		msg, err := t.execRequest(message)

		response_ch <- response{msg, err}
	}
}

func (t *TcpTransport) execRequest(message *proto.Msg) (*proto.Msg, error) {
	msg := &proto.Msg{}
	err := utils.Write(t.conn, message)
	if err != nil {
		return msg, err
	}

	msg, err = utils.Read(t.conn)
	return msg, err
}

func readMessages(r io.Reader, p []byte) error {
	for len(p) > 0 {
		n, err := r.Read(p)
		p = p[n:]
		if err != nil {
			return err
		}
	}
	return nil
}
