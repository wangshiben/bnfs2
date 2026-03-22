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
}
