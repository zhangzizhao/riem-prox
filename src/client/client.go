package main

import (
	"fmt"
	pb "github.com/golang/protobuf/proto"
	"net"
	"proto"
	"time"
	"utils"
)

func main() {
	conn, _ := net.Dial("tcp", "127.0.0.1:11237")
	fmt.Println(time.Now())
	for i := 0; i < 1; i++ {
		time.Sleep(time.Millisecond * 10)
		event := new(proto.Event)
		event.Service = pb.String("service 1")
		msg := new(proto.Msg)
		msg.Events = append(msg.Events, event)
		if err := utils.Write(conn, msg); err != nil {
			fmt.Println(err)
		} else {
			if resp, err := utils.Read(conn); err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(*(resp.Ok))
			}
		}
	}
	conn.Close()
	fmt.Println(time.Now())

}
