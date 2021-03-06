package main

import (
	"config"
	"fmt"
	"io"
	"net"
	"plog"
	"proto"
	"riemann"
	"time"
	"utils"
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
			plog.Error("listener.Accept error: ", err)
		}
	}
}

func handle(conn net.Conn) {
	for {
		success := true
		if message, err := utils.Read(conn); err != nil {
			if err != io.EOF {
				plog.Warning("utils.Read failed, err: ", err)
				//connection reset by user?
			}
			conn.Close()
			return
		} else {
			plog.Info("recieve a new msg")
			t := time.Now()
			for _, event := range message.Events {
				msg := new(proto.Msg)
				msg.Ok = message.Ok
				msg.Error = message.Error
				msg.States = message.States
				msg.Query = message.Query
				msg.XXX_unrecognized = message.XXX_unrecognized
				msg.Events = append(msg.Events, event)
				if err := riemann.Send(msg, t); err != nil {
					plog.Error("send msg failed, err: ", err)
					success = false
					break
				}
			}
		}
		resp := &proto.Msg{}
		resp.Ok = &success
		if err := utils.Write(conn, resp); err != nil {
			plog.Warning("write response failed, err: ", err)
			//broken pipe
		}
	}
}
