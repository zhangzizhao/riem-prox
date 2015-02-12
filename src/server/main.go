package main

import (
	"config"
	"fmt"
	pb "github.com/golang/protobuf/proto"
	"net"
	"plog"
	"proto"
	"riemann"
)

func main() {
	listener, err := net.Listen("tcp", config.Conf.Listen)
	if err != nil {
		fmt.Println("start failed: ", err)
		return
	}
	fmt.Println("start riemann-proxy at ", config.Conf.Listen)
	for {
		if conn, err := listener.Accept(); err == nil {
			go handle(conn)
		} else {
			plog.Warning("accept error: ", err)
		}
	}
}

func handle(conn net.Conn) {
	defer conn.Close()
	data := make([]byte, 4096)
	n, err := conn.Read(data)
	if err != nil {
		plog.Error("read from conn failed:", err)
		return
	}
	message := new(proto.Msg)
	pb.Unmarshal(data[0:n], message)

	for _, event := range message.Events {
		msg := new(proto.Msg)
		msg.Ok = message.Ok
		msg.Error = message.Error
		msg.States = message.States
		msg.Query = message.Query
		msg.XXX_unrecognized = message.XXX_unrecognized
		msg.Events = append(msg.Events, event)
		riemann.Send(msg)
	}
}
