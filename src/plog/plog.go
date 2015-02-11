package plog

import (
    "config"
    "fmt"
    "log"
    "os"
    "time"
)

//============================= private ================================
const (
    _LOG_DEBUG_FLAG = log.Ldate | log.Lmicroseconds | log.Lshortfile

    _INFO_HEADER    = "[INFO]"
    _WARNING_HEADER = "[WARNING]"
    _ERROR_HEADER   = "[ERROR]"
    _DEBUG_HEADER   = "[DEBUG]"
    _FATAL_HEADER   = "[FATAL]"
)

type postFunc func(msg string, localBuf chan string)

var accesslog *log.Logger
var errorlog *log.Logger

var post postFunc
var bufferSize = 10000
var bufMap map[string]chan string

func getPoster() postFunc {
    return func(msg string, localBuf chan string) {

        select {
        case localBuf <- msg:
            return
        case <-time.After(time.Millisecond * 1):
        }

    }
}

func initLocalLog() error {
    if access_fd, access_err := os.OpenFile(config.Conf.AccessLog,
        os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); access_err == nil {
        accesslog = log.New(access_fd, "riemann_proxy: ", _LOG_DEBUG_FLAG)
    } else {
        return access_err
    }
    if error_fd, error_err := os.OpenFile(config.Conf.ErrorLog,
        os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); error_err == nil {
        errorlog = log.New(error_fd, "riemann_proxy: ", _LOG_DEBUG_FLAG)
    } else {
        return error_err
    }
    bufMap["FatalLocal"] = make(chan string, bufferSize)
    bufMap["InfoLocal"] = make(chan string, bufferSize)
    bufMap["DebugLocal"] = make(chan string, bufferSize)
    bufMap["WarningLocal"] = make(chan string, bufferSize)
    bufMap["ErrorLocal"] = make(chan string, bufferSize)
    go localConsumeProc()

    return nil
}


func localConsumeProc() {
    defer func() {
        recover()
    }()

    for {
        select {
        case msg := <-bufMap["DebugLocal"]:
            accesslog.Println(msg)
        case msg := <-bufMap["InfoLocal"]:
            accesslog.Println(msg)
        case msg := <-bufMap["WarningLocal"]:
            errorlog.Println(msg)
        case msg := <-bufMap["ErrorLocal"]:
            errorlog.Println(msg)
        case msg := <-bufMap["FatalLocal"]:
            errorlog.Println(msg)
        }
    }
}

func init() {
    bufMap = make(map[string]chan string, 10)
    post = getPoster()
    
    if err := initLocalLog(); err != nil {
        panic(err)
    }
}

//============================= private END ================================

//============================= public API ================================
func Info(v ...interface{}) {
    post(fmt.Sprint(_INFO_HEADER, fmt.Sprint(v...)), bufMap["InfoLocal"])
}

func Infof(format string, v ...interface{}) {
    post(fmt.Sprint(_INFO_HEADER, fmt.Sprintf(format, v...)), bufMap["InfoLocal"])
}

func Debug(v ...interface{}) {
    post(fmt.Sprint(_DEBUG_HEADER, fmt.Sprint(v...)),  bufMap["DebugLocal"])
}

func Debugf(format string, v ...interface{}) {
    post(fmt.Sprint(_DEBUG_HEADER, fmt.Sprintf(format, v...)),bufMap["DebugLocal"])
}

func Warning(v ...interface{}) {
    post(fmt.Sprint(_WARNING_HEADER, fmt.Sprint(v...)), bufMap["WarningLocal"])
}

func Warningf(format string, v ...interface{}) {
    post(fmt.Sprint(_WARNING_HEADER, fmt.Sprintf(format, v...)), bufMap["WarningLocal"])
}

func Error(v ...interface{}) {
    post(fmt.Sprint(_ERROR_HEADER, fmt.Sprint(v...)), bufMap["ErrorLocal"])
}

func Errorf(format string, v ...interface{}) {
    post(fmt.Sprint(_ERROR_HEADER, fmt.Sprintf(format, v...)), bufMap["ErrorLocal"])
}

func Fatal(v ...interface{}) {
    post(fmt.Sprint(_FATAL_HEADER, fmt.Sprint(v...)), bufMap["FatalLocal"])
}

func Fatalf(format string, v ...interface{}) {
    post(fmt.Sprint(_FATAL_HEADER, fmt.Sprintf(format, v...)), bufMap["FatalLocal"])
}