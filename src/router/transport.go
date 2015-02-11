package router

import (
    "bytes"
    "encoding/binary"
    "errors"
    "io"
    "net"

    pb "github.com/golang/protobuf/proto"
    "riemann"
)

type TcpTransport struct {
    conn         net.Conn
    requestQueue chan request
}


type request struct {
    message     *riemann.Msg
    response_ch chan response
}

type response struct {
    message *riemann.Msg
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


func (t *TcpTransport) SendRecv(message *riemann.Msg) (*riemann.Msg, error) {
    response_ch := make(chan response)
    t.requestQueue <- request{message, response_ch}
    r := <-response_ch
    return r.message, r.err
}


func (t *TcpTransport) Close() error {
    close(t.requestQueue)
    err := t.conn.Close()
    if err != nil {
        return err
    }
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

func (t *TcpTransport) execRequest(message *riemann.Msg) (*riemann.Msg, error) {
    msg := &riemann.Msg{}
    data, err := pb.Marshal(message)
    if err != nil {
        return msg, err
    }
    b := new(bytes.Buffer)
    if err = binary.Write(b, binary.BigEndian, uint32(len(data))); err != nil {
        return msg, err
    }
    if _, err = t.conn.Write(b.Bytes()); err != nil {
        return msg, err
    }
    if _, err = t.conn.Write(data); err != nil {
        return msg, err
    }
    var header uint32
    if err = binary.Read(t.conn, binary.BigEndian, &header); err != nil {
        return msg, err
    }
    response := make([]byte, header)
    if err = readMessages(t.conn, response); err != nil {
        return msg, err
    }
    if err = pb.Unmarshal(response, msg); err != nil {
        return msg, err
    }
    if msg.GetOk() != true {
        return msg, errors.New(msg.GetError())
    }
    return msg, nil
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