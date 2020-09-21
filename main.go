package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/kardianos/service"
	"github.com/lengzhao/database/disk"
	"github.com/lengzhao/database/server"
	"gopkg.in/natefinch/lumberjack.v2"
)

const version = "v1.0.3"

var logger service.Logger

type program struct {
	ln     net.Listener
	closed chan bool
}

// Config service config
type Config struct {
	Address      string        `json:"address,omitempty"`
	ReadTimeout  time.Duration `json:"read_timeout,omitempty"`
	WriteTimeout time.Duration `json:"write_timeout,omitempty"`
	IdleTimeout  time.Duration `json:"idle_timeout,omitempty"`
}

func getDir() string {
	fullexecpath, err := os.Executable()
	if err != nil {
		return ""
	}

	dir, _ := filepath.Split(fullexecpath)
	return dir
}

func loadConfig() Config {
	out := Config{Address: "127.0.0.1:17777"}
	data, err := ioutil.ReadFile(path.Join(getDir(), "conf.json"))
	if err != nil {
		log.Println("fail to read file,conf.json")
		return out
	}
	err = json.Unmarshal(data, &out)
	if err != nil {
		log.Println("fail to Unmarshal configure,conf.json")
		return out
	}
	return out
}

func (p *program) Start(s service.Service) error {
	// Start should not block. Do the actual work async.
	p.closed = make(chan bool, 1)
	go p.run()
	return nil
}
func (p *program) run() {
	wd := getDir()
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	logFile := filepath.Join(wd, "log", "db.log")
	log.SetOutput(&lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    500, // megabytes
		MaxBackups: 5,
		MaxAge:     10,   //days
		Compress:   true, // disabled by default
	})
	c := loadConfig()
	log.Println("start:", c.Address, version)
	dbDir := path.Join(wd, "db_dir")
	db := server.NewRPCObj(dbDir)
	server.RegisterAPI(db, func(dir string, id uint64) server.DBApi {
		m, err := disk.Open(dir)
		if err != nil {
			log.Println("fail to open db manager,dir:", dir, err)
			return nil
		}
		return m
	})

	rpc.Register(db)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", c.Address)
	if e != nil {
		log.Fatal("fail to listen:", c.Address, e)
		return
	}
	p.ln = l
	hs := &http.Server{
		Addr:         c.Address,
		ReadTimeout:  c.ReadTimeout * time.Second,
		WriteTimeout: c.WriteTimeout * time.Second,
		IdleTimeout:  c.IdleTimeout * time.Second,
	}
	err := hs.Serve(l)
	if err != nil {
		log.Println("fail to serve rpc:", c.Address, err)
	}
	server.CloseRPCObj(db)
	log.Println("stop:", c.Address)
	p.closed <- true
}
func (p *program) Stop(s service.Service) error {
	// Stop should not block. Return with a few seconds.
	fmt.Println("stopping.(wait 5second)")
	p.ln.Close()
	select {
	case <-p.closed:
	case <-time.After(time.Second * 5):
	}

	return nil
}

func main() {
	control := flag.String("c", "", "control of service:install/start/stop/restart/uninstall")
	flag.Parse()

	log.Println("service version:", version)
	svcConfig := &service.Config{
		Name:        "govm_database",
		DisplayName: "govm_database",
		Description: "GOVM Database service",
	}

	prg := &program{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}
	logger, err = s.Logger(nil)
	if err != nil {
		log.Fatal(err)
	}

	switch *control {
	case "":
		err = s.Run()
		if err != nil {
			logger.Error(err)
		}
		s.Stop()
	case "stat":
		stat, err := s.Status()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("stat item: StatusUnknown:%d,StatusRunning:%d,StatusStopped:%d\n",
			service.StatusUnknown, service.StatusRunning, service.StatusStopped)
		log.Println("result:", stat)
	default:
		err = service.Control(s, *control)
		if err != nil {
			log.Fatal(err)
		}
	}
}
