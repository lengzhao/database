package client

import (
	"log"
	"net/rpc"
)

// Client client
type Client struct {
	dbServer string
	addrType string
	lock     chan int
	client   []*rpc.Client
}

// SetArgs Set
type SetArgs struct {
	Chain  uint64
	TbName []byte
	Key    []byte
	Value  []byte
}

// SetWithFlagArgs Set接口的入参
type SetWithFlagArgs struct {
	Chain  uint64
	Flag   []byte
	TbName []byte
	Key    []byte
	Value  []byte
}

// GetArgs Get接口的入参
type GetArgs struct {
	Chain  uint64
	TbName []byte
	Key    []byte
}

// FlagArgs flag操作的参数
type FlagArgs struct {
	Chain uint64
	Flag  []byte
}

// New new c.client
func New(addrType, serverAddr string, clientNum int) *Client {
	out := new(Client)
	out.dbServer = serverAddr
	out.addrType = addrType
	out.lock = make(chan int, clientNum)
	out.client = make([]*rpc.Client, clientNum)
	for i := 0; i < clientNum; i++ {
		out.lock <- i
	}
	return out
}

// GetAddress get address info
func (c *Client) GetAddress() (addrType, address string) {
	return c.addrType, c.dbServer
}

// Close close client
func (c *Client) Close() {
	for i := 0; i < len(c.client); i++ {
		id := <-c.lock
		if c.client[id] != nil {
			c.client[id].Close()
			c.client[id] = nil
		}
	}
	close(c.lock)
}

// OpenFlag 开启标志，标志用于记录操作，支持批量操作的回滚
func (c *Client) OpenFlag(chain uint64, flag []byte) error {
	var err error
	id, ok := <-c.lock
	if !ok {
		panic("client closed")
	}
	defer func() { c.lock <- id }()
	if c.client[id] == nil {
		c.client[id], err = rpc.DialHTTP(c.addrType, c.dbServer)
		if err != nil {
			log.Println("fail to DialHTTP.", c.addrType, c.dbServer, err)
			return err
		}
	}

	args := FlagArgs{chain, flag}
	var reply bool
	err = c.client[id].Call("TDb.OpenFlag", &args, &reply)
	if err != nil {
		log.Println("fail to OpenFlag:", c.addrType, c.dbServer, err)
		c.client[id].Close()
		c.client[id] = nil
		return err
	}

	return err
}

// GetLastFlag 获取最后一个标志
func (c *Client) GetLastFlag(chain uint64) []byte {
	var err error
	id, ok := <-c.lock
	if !ok {
		panic("client closed")
	}
	defer func() { c.lock <- id }()
	if c.client[id] == nil {
		c.client[id], err = rpc.DialHTTP(c.addrType, c.dbServer)
		if err != nil {
			log.Println("fail to DialHTTP.", c.addrType, c.dbServer, err)
			return nil
		}
	}

	var reply = make([]byte, 100)
	err = c.client[id].Call("TDb.GetLastFlag", &chain, &reply)
	if err != nil {
		log.Println("fail to GetLastFlag:", c.addrType, c.dbServer, err)
		c.client[id].Close()
		c.client[id] = nil
		return nil
	}
	return reply
}

// Commit 提交，将数据写入磁盘，标志清除
func (c *Client) Commit(chain uint64, flag []byte) error {
	var err error
	id, ok := <-c.lock
	if !ok {
		panic("client closed")
	}
	defer func() { c.lock <- id }()
	if c.client[id] == nil {
		c.client[id], err = rpc.DialHTTP(c.addrType, c.dbServer)
		if err != nil {
			log.Println("fail to DialHTTP.", c.addrType, c.dbServer, err)
			return err
		}
	}

	args := FlagArgs{chain, flag}
	var reply bool
	err = c.client[id].Call("TDb.CommitFlag", &args, &reply)
	if err != nil {
		log.Println("fail to CommitFlag:", c.addrType, c.dbServer, err)
		c.client[id].Close()
		c.client[id] = nil
		return err
	}

	return err
}

// Cancel 取消提交，将数据回滚
func (c *Client) Cancel(chain uint64, flag []byte) error {
	var err error
	id, ok := <-c.lock
	if !ok {
		panic("client closed")
	}
	defer func() { c.lock <- id }()
	if c.client[id] == nil {
		c.client[id], err = rpc.DialHTTP(c.addrType, c.dbServer)
		if err != nil {
			log.Println("fail to DialHTTP.", c.addrType, c.dbServer, err)
			return err
		}
	}

	args := FlagArgs{chain, flag}
	var reply bool
	err = c.client[id].Call("TDb.CancelFlag", &args, &reply)
	if err != nil {
		//log.Println("fail to CancelFlag:", err)
		c.client[id].Close()
		c.client[id] = nil
		return err
	}

	return err
}

// Rollback 将指定标志之后的所有操作回滚，要求当前没有开启标志
func (c *Client) Rollback(chain uint64, flag []byte) error {
	var err error
	id, ok := <-c.lock
	if !ok {
		panic("client closed")
	}
	defer func() { c.lock <- id }()
	if c.client[id] == nil {
		c.client[id], err = rpc.DialHTTP(c.addrType, c.dbServer)
		if err != nil {
			log.Println("fail to DialHTTP.", c.addrType, c.dbServer, err)
			return err
		}
	}

	args := FlagArgs{chain, flag}
	var reply bool
	err = c.client[id].Call("TDb.Rollback", &args, &reply)
	if err != nil {
		log.Println("fail to Rollback:", c.addrType, c.dbServer, err)
		c.client[id].Close()
		c.client[id] = nil
		return err
	}

	return err
}

// Set 存储数据，不携带标签，不会被回滚,tbName中的数据都别用SetWithFlag写，否则可能导致数据混乱
func (c *Client) Set(chain uint64, tbName, key, value []byte) error {
	var err error
	id, ok := <-c.lock
	if !ok {
		panic("client closed")
	}
	defer func() { c.lock <- id }()
	if c.client[id] == nil {
		c.client[id], err = rpc.DialHTTP(c.addrType, c.dbServer)
		if err != nil {
			log.Println("fail to DialHTTP.", c.addrType, c.dbServer, err)
			return err
		}
	}

	args := SetArgs{chain, tbName, key, value}
	var reply bool
	err = c.client[id].Call("TDb.Set", &args, &reply)
	if err != nil {
		log.Println("fail to TDb.Set:", c.addrType, c.dbServer, err)
		c.client[id].Close()
		c.client[id] = nil
		return err
	}

	return err
}

// SetWithFlag 写入数据，标志仅仅是一个标志，方便数据回滚
// 每个flag都有对应的historyDb文件，用于记录tbName.key的前一个标签记录位置
// 同时记录本标签最终设置的值，方便回滚
func (c *Client) SetWithFlag(chain uint64, flag, tbName, key, value []byte) error {
	var err error
	id, ok := <-c.lock
	if !ok {
		panic("client closed")
	}
	defer func() { c.lock <- id }()
	if c.client[id] == nil {
		c.client[id], err = rpc.DialHTTP(c.addrType, c.dbServer)
		if err != nil {
			log.Println("fail to DialHTTP.", c.addrType, c.dbServer, err)
			return err
		}
	}

	args := SetWithFlagArgs{chain, flag, tbName, key, value}
	var reply bool
	err = c.client[id].Call("TDb.SetWithFlag", &args, &reply)
	if err != nil {
		log.Println("fail to TDb.SetWithFlag:", c.addrType, c.dbServer, err)
		c.client[id].Close()
		c.client[id] = nil
		return err
	}

	return err
}

// Get 获取数据
func (c *Client) Get(chain uint64, tbName, key []byte) []byte {
	var err error
	id, ok := <-c.lock
	if !ok {
		panic("client closed")
	}
	defer func() { c.lock <- id }()
	if c.client[id] == nil {
		c.client[id], err = rpc.DialHTTP(c.addrType, c.dbServer)
		if err != nil {
			log.Println("fail to DialHTTP.", c.addrType, c.dbServer, err)
			return nil
		}
	}

	args := GetArgs{chain, tbName, key}
	var reply = make([]byte, 65536)
	err = c.client[id].Call("TDb.Get", &args, &reply)
	if err != nil {
		log.Println("fail to TDb.Get:", c.addrType, c.dbServer, err)
		c.client[id].Close()
		c.client[id] = nil
		return nil
	}

	return reply
}

// GetNextKey get next key
func (c *Client) GetNextKey(chain uint64, tbName, preKey []byte) []byte {
	var err error
	id, ok := <-c.lock
	if !ok {
		panic("client closed")
	}
	defer func() { c.lock <- id }()
	if c.client[id] == nil {
		c.client[id], err = rpc.DialHTTP(c.addrType, c.dbServer)
		if err != nil {
			log.Println("fail to DialHTTP.", c.addrType, c.dbServer, err)
			return nil
		}
	}

	args := GetArgs{chain, tbName, preKey}
	var reply = make([]byte, 500)
	err = c.client[id].Call("TDb.GetNextKey", &args, &reply)
	if err != nil {
		log.Println("fail to TDb.GetNextKey:", c.addrType, c.dbServer, err)
		c.client[id].Close()
		c.client[id] = nil
		return nil
	}

	return reply
}

// Exist 数据是否存在
func (c *Client) Exist(chain uint64, tbName, key []byte) bool {
	var err error
	id, ok := <-c.lock
	if !ok {
		panic("client closed")
	}
	defer func() { c.lock <- id }()
	if c.client[id] == nil {
		c.client[id], err = rpc.DialHTTP(c.addrType, c.dbServer)
		if err != nil {
			log.Println("fail to DialHTTP.", c.addrType, c.dbServer, err)
			return false
		}
	}

	args := GetArgs{chain, tbName, key}
	var reply bool
	err = c.client[id].Call("TDb.Exist", &args, &reply)
	if err != nil {
		log.Println("fail to TDb.Exist:", c.addrType, c.dbServer, err)
		c.client[id].Close()
		c.client[id] = nil
		return false
	}

	return reply
}
