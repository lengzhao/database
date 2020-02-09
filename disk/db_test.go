package disk

import (
	"bytes"
	"log"
	"os"
	"testing"
)

const testDir = "db_dir"

var (
	flag   = []byte("flag1")
	flag2  = []byte("flag2")
	flag3  = []byte("flag3")
	tbName = []byte("tbName")
	key    = []byte("key1")
	value  = []byte("value1")
	value2 = []byte("value2")
	value3 = []byte("value3")
)

func TestOpen(t *testing.T) {
	log.Println("start test:", t.Name())
	defer os.RemoveAll(testDir)
	m, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	defer m.Close()
}

func TestOpen2(t *testing.T) {
	log.Println("start test:", t.Name())
	defer os.RemoveAll(testDir)
	m1, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	m1.Close()

	m2, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	m2.Close()
}
func TestSet(t *testing.T) {
	log.Println("start test:", t.Name())
	defer os.RemoveAll(testDir)
	m, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	defer m.Close()

	err = m.Set(tbName, key, value)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v := m.Get(tbName, key)
	if bytes.Compare(v, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v)
	}
}

func TestSet1(t *testing.T) {
	log.Println("start test:", t.Name())
	defer os.RemoveAll(testDir)
	m, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	err = m.Set(tbName, key, value)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v := m.Get(tbName, key)
	if bytes.Compare(v, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v)
		return
	}
	m.Close()

	m2, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	defer m2.Close()
	v2 := m2.Get(tbName, key)
	if bytes.Compare(v2, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v2)
		return
	}
}

func TestExist(t *testing.T) {
	log.Println("start test:", t.Name())
	defer os.RemoveAll(testDir)
	m, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	defer m.Close()
	exist := m.Exist(tbName, key)
	if exist {
		t.Errorf("error result")
	}

	err = m.Set(tbName, key, value)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v := m.Get(tbName, key)
	if bytes.Compare(v, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v)
	}
	exist = m.Exist(tbName, key)
	if !exist {
		t.Errorf("error result")
	}
}

func TestSetWithFlag(t *testing.T) {
	log.Println("start test:", t.Name())
	defer os.RemoveAll(testDir)
	m, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	defer m.Close()
	err = m.OpenFlag(flag)
	if err != nil {
		t.Error("fail to open flag.", err)
		return
	}
	defer m.Commit(flag)

	err = m.SetWithFlag(flag, tbName, key, value)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v := m.Get(tbName, key)
	if bytes.Compare(v, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v)
		return
	}
}

func TestSetWithFlag2(t *testing.T) {
	log.Println("start test:", t.Name())
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)
	m, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	err = m.OpenFlag(flag)
	if err != nil {
		t.Error("fail to open flag.", err)
		return
	}

	err = m.SetWithFlag(flag, tbName, key, value)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v := m.Get(tbName, key)
	if bytes.Compare(v, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v)
		return
	}
	m.Commit(flag)
	m.Close()

	m2, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	v2 := m2.Get(tbName, key)
	if bytes.Compare(v2, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v2)
		return
	}
	m2.Close()
}

func TestSetWithFlag3(t *testing.T) {
	log.Println("start test:", t.Name())
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)
	m, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	err = m.OpenFlag(flag)
	if err != nil {
		t.Error("fail to open flag.", err)
		return
	}

	err = m.SetWithFlag(flag, tbName, key, value)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v := m.Get(tbName, key)
	if bytes.Compare(v, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v)
		return
	}
	m.Cancel(flag)
	m.Close()

	m2, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	v2 := m2.Get(tbName, key)
	if len(v2) > 0 {
		t.Errorf("error value,hope:null,get:%s", v2)
		return
	}
	m2.Close()
}

func TestSetWithFlag4(t *testing.T) {
	log.Println("start test:", t.Name())
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)
	m, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	err = m.OpenFlag(flag)
	if err != nil {
		t.Error("fail to open flag.", err)
		return
	}

	err = m.Set(tbName, key, value)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v := m.Get(tbName, key)
	if bytes.Compare(v, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v)
		return
	}
	m.Cancel(flag)
	m.Close()

	m2, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	v2 := m2.Get(tbName, key)
	if bytes.Compare(v2, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v2)
		return
	}
	m2.Close()
}

func TestRollback(t *testing.T) {
	log.Println("start test:", t.Name())
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)
	m, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	err = m.OpenFlag(flag)
	if err != nil {
		t.Error("fail to open flag.", err)
		return
	}

	err = m.SetWithFlag(flag, tbName, key, value)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v := m.Get(tbName, key)
	if bytes.Compare(v, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v)
		return
	}
	m.Commit(flag)
	m.Rollback(flag)
	v2 := m.Get(tbName, key)
	if len(v2) > 0 {
		t.Errorf("error value,hope:null,get:%s", v2)
		return
	}
	m.Close()
}

func TestRollback2(t *testing.T) {
	log.Println("start test:", t.Name())
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)
	m, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	err = m.OpenFlag(flag)
	if err != nil {
		t.Error("fail to open flag.", err)
		return
	}

	err = m.SetWithFlag(flag, tbName, key, value)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v := m.Get(tbName, key)
	if bytes.Compare(v, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v)
		return
	}
	m.Commit(flag)
	m.Rollback(flag)
	v2 := m.Get(tbName, key)
	if len(v2) > 0 {
		t.Errorf("error value,hope:null,get:%s", v2)
		return
	}
	m.Close()

	m2, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	defer m2.Close()
	v3 := m2.Get(tbName, key)
	if len(v3) > 0 {
		t.Errorf("error value,hope:null,get:%s", v3)
		return
	}
}

func TestRollback3(t *testing.T) {
	log.Println("start test:", t.Name())
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)
	m, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	err = m.OpenFlag(flag)
	if err != nil {
		t.Error("fail to open flag.", err)
		return
	}

	err = m.SetWithFlag(flag, tbName, key, value)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v := m.Get(tbName, key)
	if bytes.Compare(v, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v)
		return
	}
	m.Commit(flag)

	// flag2
	err = m.OpenFlag(flag2)
	if err != nil {
		t.Error("fail to open flag.", err)
		return
	}

	err = m.SetWithFlag(flag2, tbName, key, value2)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v2 := m.Get(tbName, key)
	if bytes.Compare(v2, value2) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v2)
		return
	}
	m.Commit(flag2)

	// flag3
	err = m.OpenFlag(flag3)
	if err != nil {
		t.Error("fail to open flag.", err)
		return
	}

	err = m.SetWithFlag(flag3, tbName, key, value3)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v3 := m.Get(tbName, key)
	if bytes.Compare(v3, value3) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v3)
		return
	}
	m.Commit(flag3)

	m.Rollback(flag3)
	v4 := m.Get(tbName, key)
	if bytes.Compare(v4, value2) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value2, v4)
		return
	}

	m.Rollback(flag2)
	v5 := m.Get(tbName, key)
	if bytes.Compare(v5, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v5)
		return
	}

	m.Rollback(flag)
	v6 := m.Get(tbName, key)
	if len(v6) > 0 {
		t.Errorf("error value,hope:null,get:%s", v6)
		return
	}
	m.Close()
}

func TestRollback4(t *testing.T) {
	log.Println("start test:", t.Name())
	defer os.RemoveAll(testDir)
	os.RemoveAll(testDir)
	m, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	err = m.OpenFlag(flag)
	if err != nil {
		t.Error("fail to open flag.", err)
		return
	}

	err = m.SetWithFlag(flag, tbName, key, value)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v := m.Get(tbName, key)
	if bytes.Compare(v, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v)
		return
	}
	m.Commit(flag)

	// flag2
	err = m.OpenFlag(flag2)
	if err != nil {
		t.Error("fail to open flag.", err)
		return
	}

	err = m.SetWithFlag(flag2, tbName, key, value2)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v2 := m.Get(tbName, key)
	if bytes.Compare(v2, value2) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v2)
		return
	}
	m.Commit(flag2)

	// flag3
	err = m.OpenFlag(flag3)
	if err != nil {
		t.Error("fail to open flag.", err)
		return
	}

	err = m.SetWithFlag(flag3, tbName, key, value3)
	if err != nil {
		t.Error("fail to set data.", err)
		return
	}
	v3 := m.Get(tbName, key)
	if bytes.Compare(v3, value3) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v3)
		return
	}
	m.Commit(flag3)
	m.Close()

	m2, err := Open(testDir)
	if err != nil {
		t.Error("fail to open dir")
		return
	}
	defer m2.Close()

	m2.Rollback(flag3)
	v4 := m2.Get(tbName, key)
	if bytes.Compare(v4, value2) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value2, v4)
		return
	}

	m2.Rollback(flag2)
	v5 := m2.Get(tbName, key)
	if bytes.Compare(v5, value) != 0 {
		t.Errorf("different value,hope:%s,get:%s", value, v5)
		return
	}

	m2.Rollback(flag)
	v6 := m2.Get(tbName, key)
	if len(v6) > 0 {
		t.Errorf("error value,hope:null,get:%s", v6)
		return
	}
}
