package config

import (
	"flag"
	"fmt"
	goyaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"net"
)

type Config struct {
	Listen       string
	RiemannAddrs []string
	ZkAddrs      []string
	AccessLog    string
	ErrorLog     string
	LocalIP      string
	NumMaxConn   int
	NumInitConn  int
	ZkPath		 string
}

var Conf Config

func init() {
	config_path := flag.String("config", "/root/conf/riemann_proxy.yaml", "config file's path")
	flag.Parse()
	if content, err := ioutil.ReadFile(*config_path); err != nil {
		return
	} else {
		if err := goyaml.Unmarshal(content, &Conf); err != nil {
			return
		}
	}
	Conf.LocalIP = localIP()
	fmt.Println(Conf)

}

func localIP() string {
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
