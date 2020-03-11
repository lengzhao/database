package disk

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"os"
	"path"
	"sync"
)

type memKey struct {
	TbName string
	Key    string
}

type memValue struct {
	tbName   []byte
	key      []byte
	value    []byte
	preFlag  []byte
	preValue []byte
	withFlag bool
}

// Manager manager
type Manager struct {
	mu     sync.Mutex
	cache  map[memKey]*memValue
	flagDb *bolt.DB
	dataDb *bolt.DB
	flag   []byte
	dir    string
}

const (
	dataFN     = "data.db"
	flagFN     = "flag.db"
	flagList   = "flag_list"
	historyMax = 20000
)

const (
	ltnValue = iota
	ltnFlag
	ltnPreValue
)

// Open open manager,if not exist,create it
func Open(dir string) (*Manager, error) {
	out := new(Manager)
	out.mu.Lock()
	defer out.mu.Unlock()
	out.dir = dir
	out.cache = make(map[memKey]*memValue)
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		err = os.Mkdir(dir, 0600)
		if err != nil {
			log.Println("create dir:", dir, err)
			return nil, err
		}
	}
	out.dataDb, err = bolt.Open(path.Join(dir, dataFN), 0600, nil)
	if err != nil {
		log.Println("fail to open file:", dir, dataFN, err)
		return nil, err
	}
	out.flagDb, err = bolt.Open(path.Join(dir, flagFN), 0600, nil)
	if err != nil {
		out.dataDb.Close()
		log.Println("fail to open file:", dir, flagFN, err)
		return nil, err
	}

	err = out.flagDb.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(flagList))
		if err != nil {
			log.Println("fail to create flagList.", err)
			return err
		}
		c := b.Cursor()
		_, v1 := c.Last()
		_, v2 := c.First()
		if bytes.Compare(v1, v2) != 0 {
			log.Println("different flag,rollback")
			lastFlag := make([]byte, len(v1))
			copy(lastFlag, v1)
			go out.Rollback(lastFlag)
		}
		return nil
	})
	if err != nil {
		out.dataDb.Close()
		out.flagDb.Close()
		log.Println("fail to open file:", dir, flagFN, err)
		return nil, err
	}
	log.Println("open database manager:", dir)
	return out, nil
}

// Close close manager
func (m *Manager) Close() {
	log.Println("start to close manager:", m.dir)
	m.mu.Lock()
	defer m.mu.Unlock()
	defer func() {
		if m.flagDb != nil {
			m.flagDb.Close()
			m.flagDb = nil
		}
		if m.dataDb != nil {
			m.dataDb.Close()
			m.dataDb = nil
		}
	}()
	if len(m.cache) > 0 {
		tx2, err := m.dataDb.Begin(true)
		if err != nil {
			log.Println("fail to begin flag file:", err)
			return
		}
		defer tx2.Rollback()
		for _, mv := range m.cache {
			if mv.withFlag {
				continue
			}
			b, err := tx2.CreateBucketIfNotExists(getLocalTableName(ltnValue, mv.tbName))
			if err != nil {
				log.Println("fail to create bucket(history value):", mv.tbName, err)
				return
			}
			err = b.Put(mv.key, mv.value)
			if err != nil {
				log.Println("fail to put bucket(value):", mv.tbName, mv.key, err)
				return
			}
		}
		tx2.Commit()
	}
	log.Println("manager closed:", m.dir)
}

func (m *Manager) getHistoryFileName(flag []byte) string {
	fn := hex.EncodeToString(flag)
	rfn := path.Join(m.dir, fn)
	return rfn + ".h"
}

// OpenFlag open flag
func (m *Manager) OpenFlag(flag []byte) error {
	if len(flag) > 100 {
		return fmt.Errorf("flag too long(<100)")
	}
	if len(flag) == 0 {
		return fmt.Errorf("try to open null flag")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.flag) != 0 {
		log.Printf("exist flag:%x,try to open:%x\n", m.flag, flag)
		return fmt.Errorf("exist flag")
	}
	rfn := m.getHistoryFileName(flag)
	if _, err := os.Stat(rfn); !os.IsNotExist(err) {
		log.Printf("exist flag file:%s,flag:%x\n", rfn, flag)
		return fmt.Errorf("exist flag file")
	}

	m.flag = flag
	m.cache = make(map[memKey]*memValue)
	return nil
}

func getLocalTableName(typ byte, tbName []byte) []byte {
	out := make([]byte, 1, len(tbName)+1)
	out[0] = typ
	out = append(out, tbName...)
	return out
}

func itoa(in uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, in)
	return out
}

func atoi(in []byte) uint64 {
	if len(in) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(in)
}

// GetLastFlag get last flag
func (m *Manager) GetLastFlag() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.flag) != 0 {
		return m.flag
	}
	var out []byte
	m.flagDb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(flagList))
		c := b.Cursor()
		_, v := c.Last()
		if v != nil {
			out = make([]byte, len(v))
			copy(out, v)
		}
		return nil
	})
	return out
}

// Commit write data to disk
func (m *Manager) Commit(flag []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.flag) == 0 {
		return fmt.Errorf("not open flag")
	}
	if bytes.Compare(flag, m.flag) != 0 {
		log.Println("try to commit different flag,")
		return fmt.Errorf("different flag")
	}
	// set last flag
	err := m.flagDb.Update(func(tx *bolt.Tx) error {
		b2 := tx.Bucket([]byte(flagList))
		c := b2.Cursor()
		last, _ := c.Last()
		next := atoi(last) + 1
		if next > historyMax {
			v := b2.Get(itoa(next - historyMax))
			if len(v) > 0 {
				fn := m.getHistoryFileName(v)
				os.Remove(fn)
			}
		}
		k := itoa(next)
		return b2.Put(k, flag)
	})
	if err != nil {
		log.Println("fail to set lastFlag.", err)
		return err
	}
	// write data to file for rollback
	rfn := m.getHistoryFileName(flag)
	history, err := bolt.Open(rfn, 0600, nil)
	if err != nil {
		log.Println("fail to open flag file:", rfn, err)
		return err
	}
	defer func() {
		if history != nil {
			history.Close()
			os.Remove(rfn)
		}
	}()
	tx1, err := history.Begin(true)
	if err != nil {
		log.Println("fail to begin flag file:", err)
		return err
	}
	defer tx1.Rollback()
	for _, mv := range m.cache {
		if !mv.withFlag {
			continue
		}
		b1, err := tx1.CreateBucketIfNotExists(getLocalTableName(ltnFlag, mv.tbName))
		if err != nil {
			log.Println("fail to create bucket(history flag):", mv.tbName, err)
			return err
		}
		err = b1.Put(mv.key, mv.preFlag)
		if err != nil {
			log.Println("fail to put bucket(flag):", mv.tbName, mv.key, err)
			return err
		}
		b2, err := tx1.CreateBucketIfNotExists(getLocalTableName(ltnPreValue, mv.tbName))
		if err != nil {
			log.Println("fail to create bucket(history preValue):", mv.tbName, err)
			return err
		}
		err = b2.Put(mv.key, mv.preValue)
		if err != nil {
			log.Println("fail to put bucket(preValue):", mv.tbName, mv.key, err)
			return err
		}
		b3, err := tx1.CreateBucketIfNotExists(getLocalTableName(ltnValue, mv.tbName))
		if err != nil {
			log.Println("fail to create bucket(history value):", mv.tbName, err)
			return err
		}
		err = b3.Put(mv.key, mv.value)
		if err != nil {
			log.Println("fail to put bucket(value):", mv.tbName, mv.key, err)
			return err
		}
	}
	tx1.Commit()
	history.Close()
	history = nil

	// write data to data.db
	tx2, err := m.dataDb.Begin(true)
	if err != nil {
		log.Println("fail to begin flag file:", err)
		return err
	}
	defer tx2.Rollback()
	for _, mv := range m.cache {
		if !mv.withFlag {
			continue
		}
		b, err := tx2.CreateBucketIfNotExists(getLocalTableName(ltnFlag, mv.tbName))
		if err != nil {
			log.Println("fail to create bucket(history flag):", mv.tbName, err)
			return err
		}
		err = b.Put(mv.key, m.flag)
		if err != nil {
			log.Println("fail to put bucket(flag):", mv.tbName, mv.key, err)
			return err
		}
	}
	for _, mv := range m.cache {
		b, err := tx2.CreateBucketIfNotExists(getLocalTableName(ltnValue, mv.tbName))
		if err != nil {
			log.Println("fail to create bucket(history value):", mv.tbName, err)
			return err
		}
		err = b.Put(mv.key, mv.value)
		if err != nil {
			log.Println("fail to put bucket(value):", mv.tbName, mv.key, err)
			return err
		}
	}
	tx2.Commit()

	// reset flag
	m.flag = nil
	m.cache = make(map[memKey]*memValue)
	err = m.flagDb.Update(func(tx *bolt.Tx) error {
		b2 := tx.Bucket([]byte(flagList))
		b2.Put(itoa(0), flag)
		return nil
	})
	if err != nil {
		log.Println("fail to update lastFlag.", err)
		return err
	}
	log.Printf("success to commit,flag:%x\n", flag)
	return nil
}

// Cancel cancel flag,not write to disk
func (m *Manager) Cancel(flag []byte) error {
	if len(m.flag) == 0 {
		return fmt.Errorf("not open flag")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if bytes.Compare(flag, m.flag) != 0 {
		log.Println("try to cancel different flag")
		return fmt.Errorf("different flag")
	}
	rfn := m.getHistoryFileName(flag)
	defer os.Remove(rfn)

	tx2, err := m.dataDb.Begin(true)
	if err != nil {
		log.Println("fail to begin flag file:", err)
		return err
	}
	defer tx2.Rollback()
	for _, mv := range m.cache {
		if mv.withFlag {
			continue
		}
		b, err := tx2.CreateBucketIfNotExists(getLocalTableName(ltnValue, mv.tbName))
		if err != nil {
			log.Println("fail to create bucket(history value):", mv.tbName, err)
			return err
		}
		err = b.Put(mv.key, mv.value)
		if err != nil {
			log.Println("fail to put bucket(value):", mv.tbName, mv.key, err)
			return err
		}
	}
	tx2.Commit()
	m.flag = nil
	m.cache = make(map[memKey]*memValue)

	return nil
}

// Rollback rollback data of flag
func (m *Manager) Rollback(flag []byte) error {
	log.Printf("rollback:%x\n", flag)
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.flag) > 0 {
		log.Println("rollback,exist opened flag,", m.flag)
		return fmt.Errorf("exist opened flag")
	}
	err := m.flagDb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(flagList))
		c := b.Cursor()
		_, v := c.Last()
		if bytes.Compare(v, flag) != 0 {
			log.Printf("different last flag,hope(in db):%x,input:%x\n",v,flag)
			return fmt.Errorf("not last flag")
		}
		return nil
	})
	if err != nil {
		log.Println("different last flag.", err)
		return err
	}

	tx2, err := m.dataDb.Begin(true)
	if err != nil {
		log.Println("fail to begin flag file:", err)
		return err
	}
	defer tx2.Rollback()
	rfn := m.getHistoryFileName(flag)
	defer os.Remove(rfn)
	history, err := bolt.Open(rfn, 0600, nil)
	if err != nil {
		log.Println("fail to open flag file:", rfn, err)
		return err
	}
	defer history.Close()
	err = history.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			typ := name[0]
			if typ == ltnValue {
				return nil
			}
			tn := name[1:]
			if typ == ltnFlag {
				b2 := tx2.Bucket(getLocalTableName(ltnFlag, tn))
				return b.ForEach(func(key, flag []byte) error {
					return b2.Put(key, flag)
				})
			}
			b2 := tx2.Bucket(getLocalTableName(ltnValue, tn))
			return b.ForEach(func(key, flag []byte) error {
				return b2.Put(key, flag)
			})
		})
	})
	tx2.Commit()

	// update last flag
	err = m.flagDb.Update(func(tx *bolt.Tx) error {
		b2 := tx.Bucket([]byte(flagList))
		c := b2.Cursor()
		c.Last()
		c.Delete()
		_, v := c.Last()
		b2.Put(itoa(0), v)
		return nil
	})
	if err != nil {
		log.Println("fail to update lastFlag.", err)
		return err
	}

	return nil
}

// SetWithFlag set data with flag, enable rollback
func (m *Manager) SetWithFlag(flag, tbName, key, value []byte) error {
	if bytes.Compare(m.flag, flag) != 0 {
		log.Printf("Set:different flag,hope:%x,error:%x\n", m.flag, flag)
		return fmt.Errorf("different flag")
	}
	// log.Printf("SetWithFlag: flag:%x,tbName:%s,key:%x,len:%d\n", flag, tbName, key, len(value))
	mk := memKey{}
	mk.TbName = hex.EncodeToString(tbName)
	mk.Key = hex.EncodeToString(key)
	m.mu.Lock()
	defer m.mu.Unlock()
	mv, ok := m.cache[mk]
	if !ok {
		mv = new(memValue)
		mv.tbName = tbName
		mv.key = key
		m.dataDb.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(getLocalTableName(ltnFlag, tbName))
			if b == nil {
				return nil
			}
			v := b.Get(key)
			if len(v) > 0 {
				mv.preFlag = make([]byte, len(v))
				copy(mv.preFlag, v)
			}
			return nil
		})
		m.dataDb.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(getLocalTableName(ltnValue, tbName))
			if b == nil {
				return nil
			}
			v := b.Get(key)
			if len(v) > 0 {
				mv.preValue = make([]byte, len(v))
				copy(mv.preValue, v)
			}
			return nil
		})
	}
	mv.value = value
	mv.withFlag = true
	m.cache[mk] = mv
	return nil
}

// Set set data, unable rollback
func (m *Manager) Set(tbName, key, value []byte) error {
	// log.Printf("Set: tbName:%s,key:%x,len:%d\n", tbName, key, len(value))
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dataDb.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(getLocalTableName(ltnValue, tbName))
		if err != nil {
			log.Printf("fail to create bucket,%s\n", tbName)
			return err
		}
		err = b.Put(key, value)
		if err != nil {
			log.Println("fail to put:", key, err)
			return err
		}
		// log.Printf("write: tbName:%s,key:%x,len:%d\n", tbName, key, len(value))
		return err
	})
}

// Get get data
func (m *Manager) Get(tbName, key []byte) []byte {
	mk := memKey{}
	mk.TbName = hex.EncodeToString(tbName)
	mk.Key = hex.EncodeToString(key)
	m.mu.Lock()
	v, ok := m.cache[mk]
	m.mu.Unlock()
	if ok {
		// log.Printf("Get: tbName:%s,key:%x,len:%d\n", tbName, key, len(v.value))
		return v.value
	}
	var out []byte
	m.dataDb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(getLocalTableName(ltnValue, tbName))
		if b == nil {
			// log.Printf("fail to get bucket:%s\n", tbName)
			return nil
		}
		v := b.Get(key)
		if len(v) > 0 {
			out = make([]byte, len(v))
			copy(out, v)
		}
		// log.Printf("read: tbName:%s,key:%x,len:%d\n", tbName, key, len(v))
		return nil
	})
	// log.Printf("Get: tbName:%s,key:%x,len:%d\n", tbName, key, len(out))

	return out
}

// Exist return true if the key exist
func (m *Manager) Exist(tbName, key []byte) bool {
	m.mu.Lock()
	mk := memKey{}
	mk.TbName = hex.EncodeToString(tbName)
	mk.Key = hex.EncodeToString(key)
	mv, ok := m.cache[mk]
	m.mu.Unlock()
	if ok && len(mv.value) > 0 {
		return true
	}

	var exist bool
	m.dataDb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(getLocalTableName(ltnValue, tbName))
		if b == nil {
			return nil
		}
		v := b.Get(key)
		if len(v) > 0 {
			exist = true
		}
		return nil
	})

	return exist
}

// GetNextKey get next key(visit database)
func (m *Manager) GetNextKey(tbName, preKey []byte) []byte {
	var out []byte
	m.dataDb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(getLocalTableName(ltnValue, tbName))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		var nk []byte
		if len(preKey) > 0 {
			nk, _ = c.Seek(preKey)
			if len(nk) == 0 {
				return nil
			}
			nk, _ = c.Next()
		} else {
			nk, _ = c.First()
		}

		if nk == nil {
			return nil
		}
		out = make([]byte, len(nk))
		copy(out, nk)
		return nil
	})

	return out
}
