package main

import (
	"fmt"
	pb "github.com/golang/protobuf/proto"
	"net"
	"proto"
	"time"
	"utils"
)

func dial(port string) net.Conn {
	ret, _ := net.Dial("tcp", "127.0.0.1:"+port)
	return ret
}

func run() {
	addrs := []string{"11237"}
	conns := make([]net.Conn, len(addrs))
	for i, addr := range addrs {
		conns[i] = dial(addr)
	}
	for i := 0; ; i++ {
		time.Sleep(time.Millisecond * 20)
		event := new(proto.Event)
		event.Service = pb.String(fmt.Sprintf("service %d", i))
		msg := new(proto.Msg)
		msg.Events = append(msg.Events, event)

		idx := i % len(addrs)

		if conns[idx] == nil {
			conns[idx] = dial(addrs[idx])
			if conns[idx] == nil {
				fmt.Println(i, " connect failed ", addrs[idx])
				continue
			}
		}

		writeSuccess := false
		if err := utils.Write(conns[idx], msg); err != nil {
			conns[idx].Close()
			conns[idx] = dial(addrs[idx])
			if conns[idx] == nil {
				fmt.Println("connect failed", addrs[idx])
				continue
			}
			if err := utils.Write(conns[idx], msg); err == nil {
				writeSuccess = true
			}
		} else {
			writeSuccess = true
		}

		if writeSuccess {
			if resp, err := utils.Read(conns[idx]); err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(*(resp.Ok))
			}
		} else {
			fmt.Println("write failed")
		}
	}
}

func main() {
	run()
}
