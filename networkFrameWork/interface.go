package networkFrameWork

import "bnfs2/network"

type NetWorkGroup interface {
	Addr() string
	ReceiveStream() network.Stream
	GetHandler(RouteName string) Handler // 保持接口定义不变
	RegisterHandler(handlerName string, handler Handler)
	Close() error
}

// 处理连接函数
type Handler func(network.Stream) error
