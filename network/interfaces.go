package network

import (
	"context"
	"io"
)

type Stream interface {
	io.Reader
	io.Writer
	Close() error
	// send And Recive Message
	SendMessage(ctx context.Context, message []byte) ([]byte, error)
	NextMessage() *Message
}

type NetCtx struct {
	Stream  Stream
	Message *Message
}

// 处理连接函数
type Handler func(ctx *NetCtx) error
