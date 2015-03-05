package utils

import (
	"bytes"
	"encoding/binary"
	pb "github.com/golang/protobuf/proto"
	"io"
	"net"
	"proto"
)

func Read(conn net.Conn) (*proto.Msg, error) {
	msg := &proto.Msg{}
	var header uint32
	if err := binary.Read(conn, binary.BigEndian, &header); err != nil {
		return msg, err
	}
	response := make([]byte, header)
	if err := readMessages(conn, response); err != nil {
		return msg, err
	}
	if err := pb.Unmarshal(response, msg); err != nil {
		return msg, err
	}
	return msg, nil
}

func Write(conn net.Conn, msg *proto.Msg) error {
	data, err := pb.Marshal(msg)
	if err != nil {
		return err
	}
	b := new(bytes.Buffer)
	if err = binary.Write(b, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	if _, err = conn.Write(b.Bytes()); err != nil {
		return err
	}
	if _, err = conn.Write(data); err != nil {
		return err
	}
	return nil
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
