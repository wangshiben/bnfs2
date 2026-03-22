package networkFrameWork

import "bnfs2/network"

type NetWorkGroup interface {
	Addr() string
	ReceiveStream() network.Stream
	GetHandler(RouteName string) network.Handler // 保持接口定义不变
	RegisterHandler(handlerName string, handler network.Handler)
	Close() error
}
