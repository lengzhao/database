package client

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"testing"

	"github.com/lengzhao/database/disk"
	"github.com/lengzhao/database/server"
)

var (
	serverAddr string
	tbName     = []byte("table1")
	flag1      = []byte("flag1")
	flag2      = []byte("flag2")
	flag3      = []byte("flag3")
	key1       = []byte("key1")
	key2       = []byte("key2")
	key3       = []byte("key3")
	value1     = []byte("value")
	value2     = []byte("value2")
	value3     = []byte("value3")
	value4     = []byte("value4")
)

func TestMain(m *testing.M) {
	fmt.Println("begin")
	os.RemoveAll("db_dir")
	defer os.RemoveAll("db_dir")
	db := server.NewRPCObj("db_dir")
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
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		log.Fatal("fail to listen:", e)
		return
	}
	go http.Serve(l, nil)
	serverAddr = l.Addr().String()
	m.Run()
	fmt.Println("end")
	l.Close()
	server.CloseRPCObj(db)
}

func TestNew(t *testing.T) {
	log.Println("start test:", t.Name())
	c := New("tcp", serverAddr, 2)
	defer c.Close()
	err := c.Set(1, tbName, key1, value1)
	if err != nil {
		t.Fatal("fail to set.", err)
	}
	v := c.Get(1, tbName, key1)
	if bytes.Compare(v, value1) != 0 {
		t.Fatal("different value:", value1, v)
	}
	// fmt.Printf("success to get value:%s\n", v)
	err = c.Set(1, tbName, key1, value2)
	if err != nil {
		t.Fatal("fail to set.", err)
	}
	v2 := c.Get(1, tbName, key1)
	if bytes.Compare(v2, value2) != 0 {
		t.Fatal("different value:", value2, v2)
	}
}

func TestFlag(t *testing.T) {
	log.Println("start test:", t.Name())
	c := New("tcp", serverAddr, 2)
	defer c.Close()
	err := c.OpenFlag(1, flag1)
	if err != nil {
		t.Fatal("fail to set.", err)
	}
	oldValue := c.Get(1, tbName, key1)
	err = c.SetWithFlag(1, flag1, tbName, key1, value1)
	if err != nil {
		t.Fatal("fail to set.", err)
	}
	v := c.Get(1, tbName, key1)
	if bytes.Compare(v, value1) != 0 {
		t.Fatal("different value:", value1, v)
	}
	// fmt.Printf("success to get value:%s\n", v)
	err = c.SetWithFlag(1, flag1, tbName, key1, value2)
	if err != nil {
		t.Fatal("fail to set.", err)
	}
	v2 := c.Get(1, tbName, key1)
	if bytes.Compare(v2, value2) != 0 {
		t.Fatal("different value:", value2, v2)
	}
	c.Cancel(1, flag1)
	v3 := c.Get(1, tbName, key1)
	if bytes.Compare(v3, oldValue) != 0 {
		t.Fatal("different value:", oldValue, v2)
	}
}

func TestSetWithFlag(t *testing.T) {
	log.Println("start test:", t.Name())
	c := New("tcp", serverAddr, 2)
	defer c.Close()

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key:%d", i)
		value := fmt.Sprintf("value:%d", i)
		err := c.Set(1, tbName, []byte(key), []byte(value))
		if err != nil {
			t.Error("error:", i, err)
		}
	}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key:%d", i)
		value := fmt.Sprintf("value:%d", i)
		v := c.Get(1, tbName, []byte(key))
		if bytes.Compare(v, []byte(value)) != 0 {
			t.Error("different value:", i)
		}
	}
}
