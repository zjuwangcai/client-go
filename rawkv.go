// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"context"
	"time"

	"github.com/kjzz/client-go/config"
	"github.com/kjzz/client-go/locate"
	"github.com/kjzz/client-go/metrics"
	"github.com/kjzz/client-go/retry"
	"github.com/kjzz/client-go/rpc"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/pd/client"
	"fmt"
)

var ErrInitIterator = errors.New("failed to init iterator")

var (
	// MaxRawKVScanLimit is the maximum scan limit for rawkv Scan.
	MaxRawKVScanLimit = 10240
	// ErrMaxScanLimitExceeded is returned when the limit for rawkv Scan is to large.
	ErrMaxScanLimitExceeded = errors.New("limit should be less than MaxRawKVScanLimit")
)

const (
	// rawBatchPutSize is the maximum size limit for rawkv each batch put request.
	rawBatchPutSize = 16 * 1024
	// rawBatchPairCount is the maximum limit for rawkv each batch get/delete request.
	rawBatchPairCount     = 512
	rawkvMaxBackoff       = 20000
	scannerNextMaxBackoff = 20000
)

const (
	scanBatchSize = 256
	batchGetSize  = 5120
)

const physicalShiftBits = 18
const txnStartKey = "_txn_start_key"

// Timeout durations.
const (
	dialTimeout               = 5 * time.Second
	readTimeoutShort          = 20 * time.Second  // For requests that read/write several key-values.
	ReadTimeoutMedium         = 60 * time.Second  // For requests that may need scan region.
	ReadTimeoutLong           = 150 * time.Second // For requests that may need scan region multiple times.
	GCTimeout                 = 5 * time.Minute
	UnsafeDestroyRangeTimeout = 5 * time.Minute

	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
)

// RawKVClient is a client of TiKV server which is used as a key-value storage,
// only GET/PUT/DELETE commands are supported.
type RawKVClient struct {
	clusterID   uint64
	regionCache *locate.RegionCache
	pdClient    pd.Client
	rpcClient   rpc.Client
}

// NewRawKVClient creates a client with PD cluster addrs.
func NewRawKVClient(pdAddrs []string, security config.Security) (*RawKVClient, error) {
	pdCli, err := pd.NewClient(pdAddrs, pd.SecurityOption{
		CAPath:   security.SSLCA,
		CertPath: security.SSLCert,
		KeyPath:  security.SSLKey,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &RawKVClient{
		clusterID:   pdCli.GetClusterID(context.TODO()),
		regionCache: locate.NewRegionCache(pdCli),
		pdClient:    pdCli,
		rpcClient:   rpc.NewRPCClient(security),
	}, nil
}

// Close closes the client.
func (c *RawKVClient) Close() error {
	c.pdClient.Close()
	return c.rpcClient.Close()
}

// ClusterID returns the TiKV cluster ID.
func (c *RawKVClient) ClusterID() uint64 {
	return c.clusterID
}

// Get queries value with the key. When the key does not exist, it returns `nil, nil`.
func (c *RawKVClient) Get(key []byte) ([]byte, error) {
	start := time.Now()
	defer func() { metrics.RawkvCmdHistogram.WithLabelValues("get").Observe(time.Since(start).Seconds()) }()

	req := &rpc.Request{
		Type: rpc.CmdRawGet,
		RawGet: &kvrpcpb.RawGetRequest{
			Key: key,
		},
	}
	resp, _, err := c.sendReq(key, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cmdResp := resp.RawGet
	if cmdResp == nil {
		return nil, errors.Trace(ErrBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return nil, errors.New(cmdResp.GetError())
	}
	if len(cmdResp.Value) == 0 {
		return nil, nil
	}
	return cmdResp.Value, nil
}

// BatchGet queries values with the keys.
func (c *RawKVClient) BatchGet(keys [][]byte) ([][]byte, error) {
	start := time.Now()
	defer func() {
		metrics.RawkvCmdHistogram.WithLabelValues("batch_get").Observe(time.Since(start).Seconds())
	}()

	bo := retry.NewBackoffer(context.Background(), rawkvMaxBackoff)
	resp, err := c.sendBatchReq(bo, keys, rpc.CmdRawBatchGet)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cmdResp := resp.RawBatchGet
	if cmdResp == nil {
		return nil, errors.Trace(ErrBodyMissing)
	}

	keyToValue := make(map[string][]byte, len(keys))
	for _, pair := range cmdResp.Pairs {
		keyToValue[string(pair.Key)] = pair.Value
	}

	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = keyToValue[string(key)]
	}
	return values, nil
}

// Put stores a key-value pair to TiKV.
func (c *RawKVClient) Put(key, value []byte) error {
	start := time.Now()
	defer func() { metrics.RawkvCmdHistogram.WithLabelValues("put").Observe(time.Since(start).Seconds()) }()
	metrics.RawkvSizeHistogram.WithLabelValues("key").Observe(float64(len(key)))
	metrics.RawkvSizeHistogram.WithLabelValues("value").Observe(float64(len(value)))

	req := &rpc.Request{
		Type: rpc.CmdRawPut,
		RawPut: &kvrpcpb.RawPutRequest{
			Key:   key,
			Value: value,
		},
	}
	resp, _, err := c.sendReq(key, req)
	if err != nil {
		return errors.Trace(err)
	}
	cmdResp := resp.RawPut
	if cmdResp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// BatchPut stores key-value pairs to TiKV.
func (c *RawKVClient) BatchPut(keys, values [][]byte) error {
	start := time.Now()
	defer func() {
		metrics.RawkvCmdHistogram.WithLabelValues("batch_put").Observe(time.Since(start).Seconds())
	}()

	if len(keys) != len(values) {
		return errors.New("the len of keys is not equal to the len of values")
	}
	bo := retry.NewBackoffer(context.Background(), rawkvMaxBackoff)
	err := c.sendBatchPut(bo, keys, values)
	return errors.Trace(err)
}

// Delete deletes a key-value pair from TiKV.
func (c *RawKVClient) Delete(key []byte) error {
	start := time.Now()
	defer func() { metrics.RawkvCmdHistogram.WithLabelValues("delete").Observe(time.Since(start).Seconds()) }()

	req := &rpc.Request{
		Type: rpc.CmdRawDelete,
		RawDelete: &kvrpcpb.RawDeleteRequest{
			Key: key,
		},
	}
	resp, _, err := c.sendReq(key, req)
	if err != nil {
		return errors.Trace(err)
	}
	cmdResp := resp.RawDelete
	if cmdResp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// BatchDelete deletes key-value pairs from TiKV
func (c *RawKVClient) BatchDelete(keys [][]byte) error {
	start := time.Now()
	defer func() {
		metrics.RawkvCmdHistogram.WithLabelValues("batch_delete").Observe(time.Since(start).Seconds())
	}()

	bo := retry.NewBackoffer(context.Background(), rawkvMaxBackoff)
	resp, err := c.sendBatchReq(bo, keys, rpc.CmdRawBatchDelete)
	if err != nil {
		return errors.Trace(err)
	}
	cmdResp := resp.RawBatchDelete
	if cmdResp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// DeleteRange deletes all key-value pairs in a range from TiKV
func (c *RawKVClient) DeleteRange(startKey []byte, endKey []byte) error {
	start := time.Now()
	var err error
	defer func() {
		var label = "delete_range"
		if err != nil {
			label += "_error"
		}
		metrics.RawkvCmdHistogram.WithLabelValues(label).Observe(time.Since(start).Seconds())
	}()

	// Process each affected region respectively
	for !bytes.Equal(startKey, endKey) {
		var resp *rpc.Response
		var actualEndKey []byte
		resp, actualEndKey, err = c.sendDeleteRangeReq(startKey, endKey)
		if err != nil {
			return errors.Trace(err)
		}
		cmdResp := resp.RawDeleteRange
		if cmdResp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		if cmdResp.GetError() != "" {
			return errors.New(cmdResp.GetError())
		}
		startKey = actualEndKey
	}

	return nil
}

// Scan queries continuous kv pairs, starts from startKey, up to limit pairs.
// If you want to exclude the startKey, append a '\0' to the key: `Scan(append(startKey, '\0'), limit)`.
func (c *RawKVClient) Scan(startKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	start := time.Now()
	defer func() { metrics.RawkvCmdHistogram.WithLabelValues("raw_scan").Observe(time.Since(start).Seconds()) }()

	if limit > MaxRawKVScanLimit {
		return nil, nil, errors.Trace(ErrMaxScanLimitExceeded)
	}

	for len(keys) < limit {
		req := &rpc.Request{
			Type: rpc.CmdRawScan,
			RawScan: &kvrpcpb.RawScanRequest{
				StartKey: startKey,
				Limit:    uint32(limit - len(keys)),
			},
		}
		resp, loc, err := c.sendReq(startKey, req)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		cmdResp := resp.RawScan
		if cmdResp == nil {
			return nil, nil, errors.Trace(ErrBodyMissing)
		}
		for _, pair := range cmdResp.Kvs {
			keys = append(keys, pair.Key)
			values = append(values, pair.Value)
		}
		startKey = loc.EndKey
		if len(startKey) == 0 {
			break
		}
	}
	return
}

func (c *RawKVClient) sendReq(key []byte, req *rpc.Request) (*rpc.Response, *locate.KeyLocation, error) {
	bo := retry.NewBackoffer(context.Background(), rawkvMaxBackoff)
	sender := rpc.NewRegionRequestSender(c.regionCache, c.rpcClient)
	for {
		loc, err := c.regionCache.LocateKey(bo, key)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		resp, err := sender.SendReq(bo, req, loc.Region, rpc.ReadTimeoutShort)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if regionErr != nil {
			err := bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			continue
		}
		return resp, loc, nil
	}
}

func (c *RawKVClient) sendBatchReq(bo *retry.Backoffer, keys [][]byte, cmdType rpc.CmdType) (*rpc.Response, error) { // split the keys
	groups, _, err := c.regionCache.GroupKeysByRegion(bo, keys)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var batches []batch
	for regionID, groupKeys := range groups {
		batches = appendKeyBatches(batches, regionID, groupKeys, rawBatchPairCount)
	}
	bo, cancel := bo.Fork()
	ches := make(chan singleBatchResp, len(batches))
	for _, batch := range batches {
		batch1 := batch
		go func() {
			singleBatchBackoffer, singleBatchCancel := bo.Fork()
			defer singleBatchCancel()
			ches <- c.doBatchReq(singleBatchBackoffer, batch1, cmdType)
		}()
	}

	var firstError error
	var resp *rpc.Response
	switch cmdType {
	case rpc.CmdRawBatchGet:
		resp = &rpc.Response{Type: rpc.CmdRawBatchGet, RawBatchGet: &kvrpcpb.RawBatchGetResponse{}}
	case rpc.CmdRawBatchDelete:
		resp = &rpc.Response{Type: rpc.CmdRawBatchDelete, RawBatchDelete: &kvrpcpb.RawBatchDeleteResponse{}}
	}
	for i := 0; i < len(batches); i++ {
		singleResp, ok := <-ches
		if ok {
			if singleResp.err != nil {
				cancel()
				if firstError == nil {
					firstError = singleResp.err
				}
			} else if cmdType == rpc.CmdRawBatchGet {
				cmdResp := singleResp.resp.RawBatchGet
				resp.RawBatchGet.Pairs = append(resp.RawBatchGet.Pairs, cmdResp.Pairs...)
			}
		}
	}

	return resp, firstError
}

func (c *RawKVClient) doBatchReq(bo *retry.Backoffer, batch batch, cmdType rpc.CmdType) singleBatchResp {
	var req *rpc.Request
	switch cmdType {
	case rpc.CmdRawBatchGet:
		req = &rpc.Request{
			Type: cmdType,
			RawBatchGet: &kvrpcpb.RawBatchGetRequest{
				Keys: batch.keys,
			},
		}
	case rpc.CmdRawBatchDelete:
		req = &rpc.Request{
			Type: cmdType,
			RawBatchDelete: &kvrpcpb.RawBatchDeleteRequest{
				Keys: batch.keys,
			},
		}
	}

	sender := rpc.NewRegionRequestSender(c.regionCache, c.rpcClient)
	resp, err := sender.SendReq(bo, req, batch.regionID, rpc.ReadTimeoutShort)

	batchResp := singleBatchResp{}
	if err != nil {
		batchResp.err = errors.Trace(err)
		return batchResp
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		batchResp.err = errors.Trace(err)
		return batchResp
	}
	if regionErr != nil {
		err := bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			batchResp.err = errors.Trace(err)
			return batchResp
		}
		resp, err = c.sendBatchReq(bo, batch.keys, cmdType)
		batchResp.resp = resp
		batchResp.err = err
		return batchResp
	}

	switch cmdType {
	case rpc.CmdRawBatchGet:
		batchResp.resp = resp
	case rpc.CmdRawBatchDelete:
		cmdResp := resp.RawBatchDelete
		if cmdResp == nil {
			batchResp.err = errors.Trace(ErrBodyMissing)
			return batchResp
		}
		if cmdResp.GetError() != "" {
			batchResp.err = errors.New(cmdResp.GetError())
			return batchResp
		}
		batchResp.resp = resp
	}
	return batchResp
}

// sendDeleteRangeReq sends a raw delete range request and returns the response and the actual endKey.
// If the given range spans over more than one regions, the actual endKey is the end of the first region.
// We can't use sendReq directly, because we need to know the end of the region before we send the request
// TODO: Is there any better way to avoid duplicating code with func `sendReq` ?
func (c *RawKVClient) sendDeleteRangeReq(startKey []byte, endKey []byte) (*rpc.Response, []byte, error) {
	bo := retry.NewBackoffer(context.Background(), rawkvMaxBackoff)
	sender := rpc.NewRegionRequestSender(c.regionCache, c.rpcClient)
	for {
		loc, err := c.regionCache.LocateKey(bo, startKey)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		actualEndKey := endKey
		if len(loc.EndKey) > 0 && bytes.Compare(loc.EndKey, endKey) < 0 {
			actualEndKey = loc.EndKey
		}

		req := &rpc.Request{
			Type: rpc.CmdRawDeleteRange,
			RawDeleteRange: &kvrpcpb.RawDeleteRangeRequest{
				StartKey: startKey,
				EndKey:   actualEndKey,
			},
		}

		resp, err := sender.SendReq(bo, req, loc.Region, rpc.ReadTimeoutShort)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if regionErr != nil {
			err := bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			continue
		}
		return resp, actualEndKey, nil
	}
}

func (c *RawKVClient) sendBatchPut(bo *retry.Backoffer, keys, values [][]byte) error {
	keyToValue := make(map[string][]byte)
	for i, key := range keys {
		keyToValue[string(key)] = values[i]
	}
	groups, _, err := c.regionCache.GroupKeysByRegion(bo, keys)
	if err != nil {
		return errors.Trace(err)
	}
	var batches []batch
	// split the keys by size and RegionVerID
	for regionID, groupKeys := range groups {
		batches = appendBatches(batches, regionID, groupKeys, keyToValue, rawBatchPutSize)
	}
	bo, cancel := bo.Fork()
	ch := make(chan error, len(batches))
	for _, batch := range batches {
		batch1 := batch
		go func() {
			singleBatchBackoffer, singleBatchCancel := bo.Fork()
			defer singleBatchCancel()
			ch <- c.doBatchPut(singleBatchBackoffer, batch1)
		}()
	}

	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			cancel()
			// catch the first error
			if err == nil {
				err = e
			}
		}
	}
	return errors.Trace(err)
}

func appendKeyBatches(batches []batch, regionID locate.RegionVerID, groupKeys [][]byte, limit int) []batch {
	var keys [][]byte
	for start, count := 0, 0; start < len(groupKeys); start++ {
		if count > limit {
			batches = append(batches, batch{regionID: regionID, keys: keys})
			keys = make([][]byte, 0, limit)
			count = 0
		}
		keys = append(keys, groupKeys[start])
		count++
	}
	if len(keys) != 0 {
		batches = append(batches, batch{regionID: regionID, keys: keys})
	}
	return batches
}

func appendBatches(batches []batch, regionID locate.RegionVerID, groupKeys [][]byte, keyToValue map[string][]byte, limit int) []batch {
	var start, size int
	var keys, values [][]byte
	for start = 0; start < len(groupKeys); start++ {
		if size >= limit {
			batches = append(batches, batch{regionID: regionID, keys: keys, values: values})
			keys = make([][]byte, 0)
			values = make([][]byte, 0)
			size = 0
		}
		key := groupKeys[start]
		value := keyToValue[string(key)]
		keys = append(keys, key)
		values = append(values, value)
		size += len(key)
		size += len(value)
	}
	if len(keys) != 0 {
		batches = append(batches, batch{regionID: regionID, keys: keys, values: values})
	}
	return batches
}

func (c *RawKVClient) doBatchPut(bo *retry.Backoffer, batch batch) error {
	kvPair := make([]*kvrpcpb.KvPair, 0, len(batch.keys))
	for i, key := range batch.keys {
		kvPair = append(kvPair, &kvrpcpb.KvPair{Key: key, Value: batch.values[i]})
	}

	req := &rpc.Request{
		Type: rpc.CmdRawBatchPut,
		RawBatchPut: &kvrpcpb.RawBatchPutRequest{
			Pairs: kvPair,
		},
	}

	sender := rpc.NewRegionRequestSender(c.regionCache, c.rpcClient)
	resp, err := sender.SendReq(bo, req, batch.regionID, rpc.ReadTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		err := bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		// recursive call
		return c.sendBatchPut(bo, batch.keys, batch.values)
	}

	cmdResp := resp.RawBatchPut
	if cmdResp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

type batch struct {
	regionID locate.RegionVerID
	keys     [][]byte
	values   [][]byte
}

type singleBatchResp struct {
	resp *rpc.Response
	err  error
}

type Iterator struct {
	batchSize    int
	version      uint64
	valid        bool
	cache        []*kvrpcpb.KvPair
	idx          int
	nextStartKey []byte
	startKey     []byte
	endKey       []byte
	lastValidKey []byte
	eof          bool
	client       *RawKVClient
	err          error
	startKeys    [][]byte
	endKeys      [][]byte
	atEnd        bool
}

func NewIterator(startKey, endKey []byte, batchSize int, client *RawKVClient, version uint64) (*Iterator, error) {
	// It must be > 1. Otherwise scanner won't skipFirst.
	if batchSize <= 1 {
		batchSize = scanBatchSize
	}
	if version == 0 {
		ver, err := client.getTimestamp(context.Background())
		if err != nil {
			return nil, err
		}
		version = ver
	}
	iterator := &Iterator{
		batchSize:    batchSize,
		valid:        true,
		nextStartKey: startKey,
		startKey:     startKey,
		endKey:       endKey,
		client:       client,
		version:      version,
		eof:          false,
	}

	// 循环获取startKeys
	bo := retry.NewBackoffer(context.WithValue(context.Background(), txnStartKey, iterator.version), scannerNextMaxBackoff)
	for {
		err := iterator.getData(bo, true)
		if err != nil {
			if err == errNoMoreRequiredDataFromTikv{
				break
			}
			return nil, err
		}
		if iterator.eof {
			break
		}
	}

	// 重新初始化iterator缓存
	iterator.valid = true
	iterator.idx = -1
	iterator.nextStartKey = startKey
	iterator.startKey = startKey
	iterator.endKey = endKey
	iterator.cache = nil
	iterator.eof = false
	iterator.err = nil

	return iterator, nil

}

func (it *Iterator) Key() []byte {
	if it.idx<len(it.cache) && it.idx >= 0 && it.valid && !it.atEnd {
		return it.cache[it.idx].Key
	}
	return nil
}

func (it *Iterator) Value() []byte {
	if it.idx<len(it.cache) && it.idx >= 0 && it.valid && !it.atEnd {
		return it.cache[it.idx].Value
	}
	return nil
}

func (it *Iterator) Seek(key []byte) bool {
	bo := retry.NewBackoffer(context.WithValue(context.Background(), txnStartKey, it.version), scannerNextMaxBackoff)
	if it.startKeys == nil || len(it.startKeys) == 0{
		return false
	}
	if bytes.Compare(key, it.lastValidKey) > 0 {
		it.Seek(it.lastValidKey)
		return it.Next()
	}
	if bytes.Compare(key, it.startKeys[0]) < 0{
		return it.Seek(it.startKeys[0])
	}

	i := 0
	for i = 1; i < len(it.startKeys); i++{
		if bytes.Compare(key, it.startKeys[i]) < 0{
			if bytes.Compare(key, it.endKeys[i-1]) > 0{
				it.nextStartKey = it.startKeys[i]
				err := it.getData(bo, false)
				if err != nil {
					return false
				}
				return true
			}
			it.nextStartKey = it.startKeys[i-1]
			err := it.getData(bo, false)
			if err != nil {
				return false
			}
			break
		}
	}
	if i == len(it.startKeys){
		it.nextStartKey = it.startKeys[i-1]
		err := it.getData(bo, false)
		if err != nil {
			return false
		}
	}

	for offset, k := range it.cache{
		if bytes.Compare(key, k.Key) <= 0{
			it.idx = offset
			if it.atEnd {
				it.unMarkAtEnd()
			}
			return true
		}
	}

	return false
}

func (it *Iterator) First() bool{
	if it.startKeys == nil || len(it.startKeys) == 0{
		return false
	}
	return it.Seek(it.startKeys[0])
}


func (it *Iterator) Last() bool{
	if it.lastValidKey == nil || len(it.lastValidKey) == 0{
		return false
	}
	return it.Seek(it.lastValidKey)
}


func (it *Iterator) Next() bool {
	bo := retry.NewBackoffer(context.WithValue(context.Background(), txnStartKey, it.version), scannerNextMaxBackoff)
	if !it.valid || it.atEnd {
		return false
	}
	for {
		it.idx++
		if it.idx >= len(it.cache) {
			if it.eof {
				it.markAtEnd()
				return false
			}
			err := it.getData(bo, false)
			if err != nil {
				if err == errNoMoreRequiredDataFromTikv {
					it.markAtEnd()
				} else {
					it.Release()
				}
				return false
			}
			if it.idx >= len(it.cache) {
				continue
			}
		}

		current := it.cache[it.idx]
		if len(it.endKey) > 0 && Key(current.Key).Cmp(Key(it.endKey)) >= 0 {
			it.eof = true
			it.Release()
			return true
		}
		// Try to resolve the lock
		if current.GetError() != nil {
			return false
		}
		return true
	}
}

func (it *Iterator) Prev() bool {
	if it.idx == -1 {
		return false
	}
	if it.idx == 0 && (len(it.startKeys) <= 1 || bytes.Compare(it.Key(), it.startKeys[0]) <= 0){
		it.idx = -1
		return false
	}
	if it.idx >= len(it.cache){
		it.idx = len(it.cache)
	}
	if it.idx > 0 {
		it.unMarkAtEnd()
		it.idx--
	} else {
		i := 0
		for i = 1; i < len(it.startKeys); i++ {
			if bytes.Compare(it.Key(), it.startKeys[i]) == 0 {
				it.nextStartKey = it.startKeys[i-1]
				break
			}
		}
		if i == len(it.startKeys) {
			return false
		}

		bo := retry.NewBackoffer(context.WithValue(context.Background(), txnStartKey, it.version), scannerNextMaxBackoff)
		err := it.getData(bo, false)
		if err != nil {
			return false
		}
		it.idx = len(it.cache) - 1
		it.eof = false
		it.unMarkAtEnd()
	}
	return true
}

func (i *Iterator) Error() error {
	return i.err
}

func (i *Iterator) Release() {
	i.valid = false
}

func (i *Iterator) markAtEnd()  {
	i.atEnd = true
}
func (i *Iterator) unMarkAtEnd()  {
	i.atEnd = false
}

func (i *Iterator) getData(bo *retry.Backoffer, init bool) error {
	sender := rpc.NewRegionRequestSender(i.client.regionCache, i.client.rpcClient)
	for {
		loc, err := i.client.regionCache.LocateKey(bo, i.nextStartKey)
		if err != nil {
			return errors.Trace(err)
		}
		req := &rpc.Request{
			Type: rpc.CmdRawScan,
			RawScan: &kvrpcpb.RawScanRequest{
				StartKey: i.nextStartKey,
				EndKey:   i.endKey,
				//Version:  i.version,
				Limit: uint32(i.batchSize),
			},
		}
		resp, err := sender.SendReq(bo, req, loc.Region, ReadTimeoutMedium)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		cmdScanResp := resp.RawScan
		if cmdScanResp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		if cmdScanResp.Size() == 0 || len(cmdScanResp.Kvs) == 0 {
			i.eof = true
			return errNoMoreRequiredDataFromTikv
		}
		i.eof = false

		kvPairs := cmdScanResp.Kvs
		i.cache, i.idx = cmdScanResp.Kvs, 0

		if init {
			//初始化时获取所有分组
			i.startKeys = append(i.startKeys, kvPairs[0].Key)
			i.endKeys = append(i.endKeys, kvPairs[len(kvPairs)-1].Key)
			i.lastValidKey = kvPairs[len(kvPairs)-1].Key
		}

		if len(kvPairs) < i.batchSize {
			// No more data in current Region. Next getData() starts
			// from current Region's endKey.
			i.nextStartKey = loc.EndKey
			if len(loc.EndKey) == 0 || (len(i.endKey) > 0 && Key(i.nextStartKey).Cmp(Key(i.endKey)) >= 0) {
				// Current Region is the last one.
				i.eof = true
			}
			return nil
		}
		// next getData() starts from the last key in kvPairs (but skip
		// it by appending a '\x00' to the key). Note that next getData()
		// may get an empty response if the Region in fact does not have
		// more data.
		lastKey := kvPairs[len(kvPairs)-1].GetKey()
		i.nextStartKey = Key(lastKey).Next()
		return nil
	}

}

func (c *RawKVClient) getTimestamp(ctx context.Context) (uint64, error) {
	physical, logical, err := c.pdClient.GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return ComposeTS(physical, logical), nil
}


// NewBatch returns a Batch instance.
func (c *RawKVClient)  NewBatch() Batch {
	return &tikvBatch{
		db: c,
	}
}

func ComposeTS(physical, logical int64) uint64 {
	return uint64((physical << physicalShiftBits) + logical)
}

// TiKV Batch
type Batch interface {
	Put(key, value []byte) error
	Delete(key []byte) error
	Write() error
	Len() int
}

type KeyValuePair struct {
	Key   []byte
	Value []byte
}

// tikvBatch implements the Batch interface.
type tikvBatch struct {
	db     *RawKVClient
	keys   [][]byte
	values [][]byte
}

// Put appends 'put operation' of the given K/V pair to the batch.
func (b *tikvBatch) Put(key, value []byte) error {
	b.keys = append(b.keys, key)
	b.values = append(b.values, value)
	return nil
}

// TODO check it
// Delete appends 'delete operation' of the given key to the batch.
func (b *tikvBatch) Delete(key []byte) error {
	for i := 0; i < len(b.keys); i++ {
		if bytes.Compare(key, b.keys[i]) == 0 {
			b.keys = append(b.keys[:i], b.keys[i+1:]...)
			return nil
		}
	}
	return nil
}

// Write apply the given batch to the DB.
func (b *tikvBatch) Write() error {
	if b.db == nil {
		panic(fmt.Errorf("no db specified"))
	}
	return b.db.BatchPut(b.keys, b.values)
}

// Len returns number of records in the batch.
func (b *tikvBatch) Len() int {
	return len(b.keys)
}

func (b *tikvBatch) Parse() []KeyValuePair {
	kv := make([]KeyValuePair, len(b.keys))
	for i := 0; i < len(b.keys); i++ {
		kv[i].Key = b.keys[i]
		kv[i].Value = b.values[i]
	}
	return kv
}