package StoreNode

import (
	"bnfs2/KVStore"
	"bnfs2/interfaces"
	messagequeue "bnfs2/messageQueue"
	"bnfs2/network"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var storeDb KVStore.KvDB

func InitKvDBIfNotInit() {
	if storeDb == nil {
		storeDb, _ = KVStore.New("")
	}
}

// writeSliceToLocal 将文件切片保存到本地存储
func writeSliceToLocal(payLoad []byte) error {
	// TODO: 按照扇区进行存储
	// TODO: 文件头应当附带节点信息以及对文件签名的信息
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
		// TODO: 写入完成后推入mq进行广播，进行区块广播以及扇区哈希计算
		sign, err := Node.Sign(message.Payload)
		if err != nil {
			return err
		}
		messagequeue.MQ.PushMessage(messagequeue.MessageBroadcast, messagequeue.MqMessage{
			Data:   message.Payload,
			Sign:   sign,
			PubKey: Node.Pubkey(),
		})
		return nil
	}
}

func BroadcastSlice(LocalDHT interfaces.DHTTable) messagequeue.QueuenHanlder {
	return func(data *messagequeue.MqMessage) {
		heads := LocalDHT.GetBuketHead()
		group := &sync.WaitGroup{}
		for index, head := range heads {
			group.Add(1)
			go func(head interfaces.Node, index int) {
				defer group.Done()
				ctx := context.Background()
				message := &network.Message{
					Header: &network.Header{
						RouteName:     "",
						PayLoadLength: 0,
						OriginData:    nil,
					},
					// 使用GRPC重构Payload
					Payload: data.Data,
				}
				bytes, err := message.ParseToBytes()
				if err != nil {
					return
				}
				// 发送消息
				head.GetStream().SendMessage(ctx, bytes)
				// TODO: 接收响应，直到响应成功或者失败，如果失败，重新获取该桶的第一个节点(存活时间最长)

			}(head, index)
		}
		group.Wait()
	}

}
