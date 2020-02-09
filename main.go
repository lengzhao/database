package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"path/filepath"

	"github.com/kardianos/service"
	"github.com/lengzhao/database/disk"
	"github.com/lengzhao/database/server"
	"gopkg.in/natefinch/lumberjack.v2"
)

var logger service.Logger

type program struct {
	ln net.Listener
}

// Config service config
type Config struct {
	Address string `json:"address,omitempty"`
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
	out := Config{"127.0.0.1:9191"}
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
	log.Println("start:", c.Address)
	dbDir := path.Join(wd, "db_dir")
	db := server.NewRPCObj(dbDir)
	server.RegisterAPI(db, func(dir string) server.DBApi {
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
	err := http.Serve(l, nil)
	if err != nil {
		log.Println("fail to serve rpc:", c.Address, err)
	}
	server.CloseRPCObj(db)
	log.Println("stop:", c.Address)
}
func (p *program) Stop(s service.Service) error {
	// Stop should not block. Return with a few seconds.
	p.ln.Close()
	return nil
}

func main() {
	control := flag.String("c", "", "control of service:install/start/stop/restart/uninstall")
	flag.Parse()

	svcConfig := &service.Config{
		Name:        "GOVM Database",
		DisplayName: "GOVM Database",
		Description: "GOVM Database service.",
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

	if *control != "" {
		err = service.Control(s, *control)
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	err = s.Run()
	if err != nil {
		logger.Error(err)
	}
}
