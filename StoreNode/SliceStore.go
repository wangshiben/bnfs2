package StoreNode

import (
	"bnfs2/KVStore"
	"bnfs2/interfaces"
	"bnfs2/network"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

var storeDb KVStore.KvDB

func InitKvDBIfNotInit() {
	if storeDb == nil {
		storeDb, _ = KVStore.New("")
	}
}

// writeSliceToLocal 将文件切片保存到本地存储
func writeSliceToLocal(payLoad []byte) error {
	storageDir, err := storeDb.Get(KVStore.StoreLocation)
	if err != nil {
		return err
	}

	hasher := sha256.New()
	_, err = hasher.Write(payLoad)
	if err != nil {
		fmt.Printf("failed to write to hasher: %v", err.Error())
	}

	// 获取二进制哈希值并转为 Hex 字符串
	hashBytes := hasher.Sum(nil)
	fileName := hex.EncodeToString(hashBytes)

	// 构建完整文件路径
	filePath := filepath.Join(storageDir, fileName)

	// 2. 检查文件是否存在
	_, err = os.Stat(filePath)
	if err == nil {
		// 发送响应: 文件已存在，开始 挑战-响应模型
		return nil
	} else if !os.IsNotExist(err) {
		// 发生了其他错误（如权限问题），不是简单的“文件不存在”
		return err
	}

	// 3. 文件不存在，执行保存操作
	// 确保存储目录存在
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		return err
	}

	// 写入文件，权限设置为 0644 (用户读写，组和其他只读)
	if err := os.WriteFile(filePath, payLoad, 0644); err != nil {
		return err
	}
	return nil
}
func StoreSlice(Node interfaces.Node) network.Handler {
	InitKvDBIfNotInit()
	return func(ctx *network.NetCtx) error {
		message := ctx.Message
		err := writeSliceToLocal(message.Payload)
		if err != nil {
			return err
		}
		// 开始将文件切片进行广播
		return nil
	}
}
