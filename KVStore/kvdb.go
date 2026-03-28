package KVStore

import (
	"errors"
	"github.com/dgraph-io/badger/v4"
)

// KvDB 是基于 BadgerDB 实现的键值存储结构
type KvDBImp struct {
	db *badger.DB
}

const StoreLocation = "sliceLocation"
const BaseStoreLocation = "baseStoreLocation"

// New 创建一个新的 KvDB 实例
// path: db 文件存储的路径
func New(path string) (KvDB, error) {
	if len(path) == 0 {
		path = DefaultDbRoot
	}
	opts := badger.DefaultOptions(path)
	// 根据需求，所有读写操作均基于此文件，使用默认配置即可
	// 如需高性能可调整 SyncWrites 等选项，此处保持默认以保证数据安全
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	k := &KvDBImp{db: db}
	k.Put(StoreLocation, DefaultStoreLocate)
	return k, nil
}

// Put 写入键值对
func (k *KvDBImp) Put(key, value string) error {
	return k.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte(value))
	})
}

// Get 读取键对应的值
// 如果键不存在，返回 error
func (k *KvDBImp) Get(key string) (string, error) {
	var val []byte
	err := k.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return string(val), err
}

// Delete 删除指定的键
func (k *KvDBImp) Delete(key string) error {
	return k.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// Close 关闭数据库连接
func (k *KvDBImp) Close() error {
	if k.db != nil {
		return k.db.Close()
	}
	return errors.New("db is nil")
}
