package StoreNode

import (
	"bnfs2/KVStore"
	"bnfs2/network"
	"context"
	"os"
	"path/filepath"
)

// DownLoadSlice 上传文件的切片，如果不存在则返回Error以及二进制error
func DownLoadSlice(SliceName, SectorName string) ([]byte, error) {
	BaseLocation, err := storeDb.Get(KVStore.BaseStoreLocation)
	if err != nil {
		return nil, err
	}
	finalFilePath := filepath.Join(BaseLocation, SectorName, SliceName)
	return ReadFile(finalFilePath)
}
func ReadFile(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()
	fileBytes := make([]byte, fileSize)
	_, err = file.Read(fileBytes)
	if err != nil {
		return nil, err
	}
	return fileBytes, err
}
func DownLoadHandler() network.Handler {
	return func(ctx *network.NetCtx) error {
		payload := ctx.Message.Payload
		// TODO: 获取文件切片名称以及扇区名称
		// payLoad前20字节是文件切片名称，后20字节是扇区名称
		sliceName := payload[:20]
		sectorName := payload[20:40]
		slice, err := DownLoadSlice(string(sliceName), string(sectorName))
		if err != nil {
			return err
		}
		cont := context.Background()
		messg := &network.Message{
			Header:  &network.Header{},
			Payload: slice,
		}
		bytes, err := messg.ParseToBytes()
		if err != nil {
			return err
		}
		_, err = ctx.Stream.SendMessage(cont, bytes)
		if err != nil {
			return err
		}
		return err
	}
}
