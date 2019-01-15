package tikv

import (
	"context"
	"fmt"
	"github.com/kjzz/client-go/config"
	"github.com/stretchr/testify/assert"
	"testing"
	"strconv"
)

var cli *RawKVClient

type innerKV struct {
	Key []byte
	Val []byte
}

const pd0_addr = "172.16.3.6:2379"

var expect_0_15 = []innerKV{
	{[]byte("hyperchain-0"), []byte("zero")},
	{[]byte("hyperchain-1"), []byte("one")},
	{[]byte("hyperchain-2"), []byte("two")},
	{[]byte("hyperchain-3"), []byte("three")},
	{[]byte("hyperchain-4"), []byte("four")},
	{[]byte("hyperchain-5"), []byte("five")},
	{[]byte("hyperchain-6"), []byte("six")},
	{[]byte("hyperchain-7"), []byte("seven")},
	{[]byte("hyperchain-8"), []byte("eight")},
	{[]byte("hyperchain-9"), []byte("nine")},
	{[]byte("hyperchain-10"), []byte("ten")},
	{[]byte("hyperchain-11"), []byte("eleven")},
	{[]byte("hyperchain-12"), []byte("twelve")},
	{[]byte("hyperchain-13"), []byte("thirteen")},
	{[]byte("hyperchain-14"), []byte("fourteen")},
	{[]byte("hyperchain-15"), []byte("fifteen")},
}

func init() {
	client, err := NewRawKVClient([]string{pd0_addr}, config.Security{})
	if err != nil {
		fmt.Println(err.Error())
	}
	cli = client
}

func TestNewRawKVClient(t *testing.T) {
	client, err := NewRawKVClient([]string{pd0_addr}, config.Security{})
	if err != nil {
		t.Error(err)
	}
	err = client.Put([]byte("hyperchain"), []byte("hyperchain"))
	if err != nil {
		t.Error()
	}
	value, err := client.Get([]byte("hyperchain"))
	if err != nil {
		t.Error()
	}
	t.Log("Value:", string(value))
}

func TestRawKVClient_Put(t *testing.T) {
	for _, kv := range expect_0_15 {
		err := cli.Put(kv.Key, kv.Val)
		assert.NoError(t, err)
	}

	for _, kv := range expect_0_15 {
		val, err := cli.Get(kv.Key)
		assert.NoError(t, err)
		assert.Equal(t, kv.Val, val)
	}
}

func TestGetVersion(t *testing.T) {
	ver, err := cli.getTimestamp(context.Background())
	if err != nil {
		t.Error()
	}
	t.Log(ver)
}

func TestNewIterator(t *testing.T) {
	it, err := NewIterator([]byte("hyperchain-"), []byte("hyperchain-a"), 0, cli, 0)
	if err != nil {
		t.Error(err.Error())
	}
	t.Log(string(it.Key()))
	bool := it.Next()
	if !bool {
		t.Error("error iter next")
	}
	t.Log(string(it.Key()))
	bool = it.Prev()
	if !bool {
		t.Error("error iter next")
	}
	t.Log(string(it.Key()))
	bool = it.Next()
	bool = it.Next()
	bool = it.Next()
	bool = it.Next()
	if !bool {
		t.Error("error iter next")
	}
	t.Log(string(it.Key()))
}

func TestBigIterator(t *testing.T)  {
	prefix := "hpc-big_iterator_test-"

	for i:=0; i<=1024; i++{
		key := []byte(prefix+strconv.Itoa(i))
		value := []byte(strconv.Itoa(i*1000))
		err := cli.Put(key, value)
		assert.NoError(t, err)
	}

	it, err := NewIterator([]byte(prefix), []byte(prefix+"a"), 0, cli, 0)

	if err != nil {
		t.Error(err.Error())
	}

	for i:=0; i<1025; i++{
		t.Log(string(it.Key()), it.idx, i)
		bol := it.Next()
		if !bol {
			t.Errorf("error iter next %v %v %v %v", string(it.Key()), it.idx, string(it.endKey), i)
		}
	}

	for i:=0; i<5; i++{
		t.Log(string(it.Key()), it.idx, i)
		bol := it.Next()
		if bol {
			t.Errorf("error iter next %v %v %v %v", string(it.Key()), it.idx, string(it.endKey), i)
		}
	}

	t.Log("next finish")

	for i:=0; i<1025; i++{
		t.Log(string(it.Key()), i, it.idx)
		bol := it.Prev()
		if !bol {
			t.Error("error iter prev")
		}
	}

	for i:=0; i<5; i++{
		t.Log(string(it.Key()), i, it.idx)
		bol := it.Prev()
		if bol {
			t.Error("error iter prev")
		}
	}
}