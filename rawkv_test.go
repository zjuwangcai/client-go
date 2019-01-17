package tikv

import (
	"context"
	"fmt"
	"github.com/kjzz/client-go/config"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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
	assert.Equal(t, it.Next(), true)
	assert.Equal(t, it.Next(), true)
	assert.Equal(t, it.Next(), true)
	assert.Equal(t, it.Key(), []byte("hyperchain-10"))

	assert.Equal(t, it.Prev(), true)
	assert.Equal(t, it.Key(), []byte("hyperchain-1"))

	assert.Equal(t, it.First(), true)
	assert.Equal(t, it.Key(), []byte("hyperchain-0"))
	assert.Equal(t, it.Last(), true)
	assert.Equal(t, it.Key(), []byte("hyperchain-9"))
}

func TestBigIterator(t *testing.T)  {
	prefix := "hpc-big_iterator_test1-"

	for i:=0; i<1029; i++{
		key := []byte(prefix+strconv.Itoa(i))
		value := []byte(strconv.Itoa(i*1000))
		err := cli.Put(key, value)
		assert.NoError(t, err)
	}

	it, err := NewIterator([]byte(prefix), []byte(prefix+"a"), 0, cli, 0)

	if err != nil {
		t.Error(err.Error())
	}

	for i:=0; i<1029; i++{
		t.Log(string(it.Key()), it.idx, i)
		bol := it.Next()
		if !bol {
			t.Errorf("error iter next %v %v %v %v", string(it.Key()), it.idx, string(it.endKey), i)
		}
	}

	for i:=0; i<3; i++{
		t.Log(string(it.Key()), it.idx, i)
		bol := it.Next()
		if bol {
			t.Errorf("error iter next %v %v %v %v", string(it.Key()), it.idx, string(it.endKey), i)
		}
	}

	t.Log("next finish")

	for i:=0; i<1029; i++{
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

func TestBigIteratorIntegralBatch(t *testing.T)  {
	prefix := "hpc-big_iterator_test2-"

	start := time.Now()
	for i:=0; i<1024; i++{
		key := []byte(prefix+strconv.Itoa(i))
		value := []byte(strconv.Itoa(i*1000))
		err := cli.Put(key, value)
		assert.NoError(t, err)
	}
	end := time.Now()

	dur := end.Sub(start).Seconds()
	t.Log(dur)

	it, err := NewIterator([]byte(prefix), []byte(prefix+"a"), 0, cli, 0)

	if err != nil {
		t.Error(err.Error())
	}

	for i:=0; i<1024; i++{
		bol := it.Next()
		if !bol {
			t.Errorf("error iter next %v %v %v %v", string(it.Key()), it.idx, string(it.endKey), i)
		}
		t.Log(string(it.Key()), it.idx, i)
	}

	for i:=0; i<3; i++{
		t.Log(string(it.Key()), it.idx, i)
		bol := it.Next()
		if bol {
			t.Errorf("error iter next %v %v %v %v", string(it.Key()), it.idx, string(it.endKey), i)
		}
	}

	t.Log("next finish")

	for i:=0; i<1024; i++{
		bol := it.Prev()
		if !bol {
			t.Error("error iter prev")
		}
		t.Log(string(it.Key()), i, it.idx)
	}

	for i:=0; i<5; i++{
		t.Log(string(it.Key()), i, it.idx)
		bol := it.Prev()
		if bol {
			t.Error("error iter prev")
		}
	}

}


func TestIteratorNextAfterCrossed(t *testing.T)  {
	prefix := "hpc-big_iterator_test3-"

	start := time.Now()
	for i:=0; i<1024; i++{
		key := []byte(prefix+strconv.Itoa(i))
		value := []byte(strconv.Itoa(i*1000))
		err := cli.Put(key, value)
		assert.NoError(t, err)
	}
	end := time.Now()

	dur := end.Sub(start).Seconds()
	t.Log(dur)

	it, err := NewIterator([]byte(prefix), []byte(prefix+"a"), 0, cli, 0)

	if err != nil {
		t.Error(err.Error())
	}

	for i:=0; i<1024; i++{
		bol := it.Next()
		if !bol {
			t.Errorf("error iter next %v %v %v %v", string(it.Key()), it.idx, string(it.endKey), i)
		}
		t.Log(string(it.Key()), it.idx, i)
	}

	for i:=0; i<3; i++{
		t.Log(string(it.Key()), it.idx, i)
		bol := it.Next()
		if bol {
			t.Errorf("error iter next %v %v %v %v", string(it.Key()), it.idx, string(it.endKey), i)
		}
	}

	t.Log("next finish")

	for i:=0; i<1024; i++{
		bol := it.Prev()
		if !bol {
			t.Error("error iter prev")
		}
		t.Log(string(it.Key()), i, it.idx)
	}

	for i:=0; i<5; i++{
		t.Log(string(it.Key()), i, it.idx)
		bol := it.Prev()
		if bol {
			t.Error("error iter prev")
		}
	}

	for i:=0; i<1024; i++{
		bol := it.Next()
		if !bol {
			t.Errorf("error iter next %v %v %v %v", string(it.Key()), it.idx, string(it.endKey), i)
		}
		t.Log(string(it.Key()), it.idx, i)
	}
}

func TestIteratorSeek(t *testing.T)  {
	prefix := "hpc-big_iterator_test5-"

	start := time.Now()
	for i:=1; i<100; i++{
		if i!= 55 {
			key := []byte(prefix + strconv.Itoa(i))
			value := []byte(strconv.Itoa(i * 1000))
			err := cli.Put(key, value)
			assert.NoError(t, err)
		}
	}
	end := time.Now()

	dur := end.Sub(start).Seconds()
	t.Log(dur)

	it, err := NewIterator([]byte(prefix), []byte(prefix+"a"), 0, cli, 0)

	if err != nil {
		t.Error(err.Error())
	}

	assert.Equal(t, it.Seek([]byte(prefix+"99")), true)
	assert.Equal(t, it.Key(), []byte(prefix+"99"))
	assert.Equal(t, it.Next(), false)
	assert.Equal(t, it.Prev(), true)
	assert.Equal(t, it.Prev(), true)
	assert.Equal(t, it.Key(), []byte(prefix+"98"))

	assert.Equal(t, it.Seek([]byte(prefix+"1")), true)
	assert.Equal(t, it.Key(), []byte(prefix+"1"))
	assert.Equal(t, it.Prev(), false)
	assert.Equal(t, it.Next(), true)
	assert.Equal(t, it.Next(), true)
	assert.Equal(t, it.Key(), []byte(prefix+"10"))

	assert.Equal(t, it.Seek([]byte(prefix+"55")), true)
	assert.Equal(t, it.Key(), []byte(prefix+"56"))
	assert.Equal(t, it.Prev(), true)
	assert.Equal(t, it.Key(), []byte(prefix+"54"))

	assert.Equal(t, it.Seek([]byte(prefix+"999")), false)
	assert.Equal(t, it.Prev(), true)
	assert.Equal(t, it.Key(), []byte(prefix+"99"))

	assert.Equal(t, it.Seek([]byte(prefix+"0")), true)
	assert.Equal(t, it.Key(), []byte(prefix+"1"))
	assert.Equal(t, it.Next(), true)
	assert.Equal(t, it.Key(), []byte(prefix+"10"))


	assert.Equal(t, it.Seek([]byte(prefix+"55")), true)
	for it.Next(){
		t.Log("it key next :", string(it.Key()))
	}
	assert.Equal(t, it.Prev(), true)
	assert.Equal(t, it.Key(), []byte(prefix+"99"))

	assert.Equal(t, it.Seek([]byte(prefix+"55")), true)
	for it.Prev(){
		t.Log("it key prev :", string(it.Key()))
	}
	assert.Equal(t, it.Next(), true)
	assert.Equal(t, it.Key(), []byte(prefix+"1"))

}