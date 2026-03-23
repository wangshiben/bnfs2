package messagequeue

import "sync"

type iocContainer struct {
	dataMap     map[string]interface{}
	lock        sync.RWMutex
	channelMap  map[string]chan interface{}
	funcLock    sync.RWMutex
	callFuncMap map[string]callFunc
}
type callFunc func(data interface{})

func (i *iocContainer) addData(key string, data interface{}) {
	i.lock.Lock()
	defer i.lock.Unlock()
	if i.callFuncMap[key] != nil {
		i.channelMap[key] <- data // 发送data
	}
	i.dataMap[key] = data
}

func (i *iocContainer) getIOCData(key string) interface{} {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.dataMap[key]
}

func (i *iocContainer) registerFunc(key string, called callFunc) {
	i.funcLock.Lock()
	defer i.funcLock.Unlock()
	i.callFuncMap[key] = called
	i.channelMap[key] = make(chan interface{})
	go i.startListen(key)

}
func (i *iocContainer) startListen(key string) {
	i.funcLock.RLock()
	ch := i.channelMap[key]
	fn := i.callFuncMap[key]
	i.funcLock.RUnlock()
	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()
	for data := range ch {
		// 执行回调函数
		// 如果 fn(data) 发生 panic，会被上面的 defer 捕获并记录
		fn(data)
	}
}
func newIocContainer() *iocContainer {
	return &iocContainer{
		dataMap:     make(map[string]interface{}),
		lock:        sync.RWMutex{},
		channelMap:  make(map[string]chan interface{}),
		funcLock:    sync.RWMutex{},
		callFuncMap: make(map[string]callFunc),
	}
}
