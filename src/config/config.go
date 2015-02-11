package config

import (
    "flag"
    "io/ioutil"
    "strings"
    goyaml "gopkg.in/yaml.v2"
    "fmt"
    "github.com/samuel/go-zookeeper/zk"
    "strconv"
    "net"
    "time"
)

type Config struct {
    Listen string
    RiemannAddrs []string
    ZkAddrs []string
    DeadRiemann []bool
    DeadLocal []bool
    AccessLog string
    ErrorLog string
    localIP string
}
const (
    ZK_PATH = "/services/proxy/riemann/dead"
)

var Conf Config

func watch(conn *zk.Conn, path string) (chan []string, chan error) {
    snapshots := make(chan []string)
    errors := make(chan error)
    go func() {
        for {
            snapshot, _, events, err := conn.ChildrenW(path)
            if err != nil {
                errors <- err
                return
            }
            snapshots <- snapshot
            evt := <-events
            if evt.Err != nil {
                errors <- evt.Err
                return
            }
        }
    }()
    return snapshots, errors
}

func tryCreatePath(path string, conn *zk.Conn) {
    if exists, _, err := conn.Exists(path); err == nil && exists {
        if children, _, err := conn.Children(path); err == nil {
            updateRiemannStatus(children)
        }
        return
    }
    flags := int32(0)
    acl := zk.WorldACL(zk.PermAll)
    for i:=1; i<=len(path); i++ {
        if path[i] == '/' || i == len(path) {
            if exists, _, err := conn.Exists(path[:i]); err == nil && !exists {
                conn.Create(path[:i], []byte(""), flags, acl)
            }
        }
    }

}
func updateRiemannStatus(children []string) {
    dead := make([]bool, len(Conf.DeadRiemann))
    local := make([]bool, len(Conf.DeadLocal))
    for _, val := range children {
        s := strings.Split(val, "-")
        if idx, err := strconv.Atoi(s[len(s)-1]); err == nil {
            dead[idx] = true
            if s[0] == Conf.localIP {
                local[idx] = true
            }
        }
    }
    alive := false
    for i, val := range dead {
        Conf.DeadRiemann[i] = val
        Conf.DeadLocal[i] = local[i]
        if !val {
            alive = true
        }
    }
    if !alive {
        panic("all riemann dead")
    }
    fmt.Println("riemann status updated:", Conf.DeadRiemann)
}

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
    Conf.DeadRiemann = make([]bool, len(Conf.RiemannAddrs))
    Conf.DeadLocal = make([]bool, len(Conf.RiemannAddrs))
    Conf.localIP = localIP()
    fmt.Println(Conf)

    conn, _, err := zk.Connect(Conf.ZkAddrs, time.Second*10)
    if err != nil {
        fmt.Println("can not connect to zk, use local status")
        return
    }
    
    tryCreatePath(ZK_PATH, conn)

    snapshots, errors := watch(conn, ZK_PATH)
    go func() {
        for {
            select {
            case snapshot := <- snapshots:
                updateRiemannStatus(snapshot)
            case err := <-errors:
                panic(err)
            }
        }
    }()
}

func MarkDead(idx int) {
    Conf.DeadRiemann[idx] = true
    conn, _, err := zk.Connect(Conf.ZkAddrs, time.Second*10)
    defer conn.Close()
    if err == nil {
        conn.Create(ZK_PATH + "/" + Conf.localIP + "-" + strconv.Itoa(idx), []byte(""), int32(0), zk.WorldACL(zk.PermAll))
    }
}

func MarkAlive(idx int) {
    if !Conf.DeadLocal[idx] {
        return
    }
    Conf.DeadRiemann[idx] = false
    conn, _, err := zk.Connect(Conf.ZkAddrs, time.Second*10)
    defer conn.Close()
    if err == nil {
        conn.Delete(ZK_PATH + "/" + Conf.localIP + "-" + strconv.Itoa(idx), -1)
    }
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