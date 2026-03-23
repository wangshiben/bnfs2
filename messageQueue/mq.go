package messagequeue

// 单通道消息队列简单使用
import (
	"sync"
)

var MQ *MessageQueue = &MessageQueue{
	quens:        make(map[string]*MqQueue),
	lock:         sync.Mutex{},
	iocContainer: newIocContainer(),
	rwLock:       sync.RWMutex{},
}

type MessageQueue struct {
	lock         sync.Mutex // 锁，当需要新建mqQueue时加锁
	quens        map[string]*MqQueue
	rwLock       sync.RWMutex
	iocContainer *iocContainer // 数据容器
}

const (
	QueueHasClosed     = mqError("queue has closed")
	QueueHaveNoHandler = mqError("queue have no handler")
	HandlerHasRegister = mqError("handler has register")
	QueueNameIsExist   = mqError("queue name is exist,but it not a queue")
)

type mqError string

func (e mqError) Error() string {
	return string(e)
}

func (mq *MessageQueue) AddIOCData(name string, data interface{}) {
	mq.rwLock.Lock()
	defer mq.rwLock.Unlock()
	mq.iocContainer.addData(name, data)
}
func (mq *MessageQueue) GetIOCData(name string) interface{} {
	mq.rwLock.RLock()
	defer mq.rwLock.RUnlock()
	return mq.iocContainer.getIOCData(name)
}
func (mq *MessageQueue) RegisterIOCDataHandler(name string, called callFunc) {
	mq.iocContainer.registerFunc(name, called)
}
func (mq *MessageQueue) PushMessageQueue(name string, data interface{}) error {
	iocData := mq.GetIOCData(name)
	if iocData != nil {
		queue, isQueue := iocData.(*Queue)
		if !isQueue {
			return QueueNameIsExist
		}
		queue.Append(data)
		// mq.AddIOCData(name, queue)
		return nil
	}
	queue := NewQueue()
	queue.Append(data)
	mq.AddIOCData(name, queue)
	return nil
}
func (mq *MessageQueue) AddQueue(name string, data *Queue) {
	mq.AddIOCData(name, data)
}
func (mq *MessageQueue) GetQueue(name string) *Queue {
	iocData := mq.GetIOCData(name)
	if iocData != nil {
		queue, isQueue := iocData.(*Queue)
		if !isQueue {
			return nil
		}
		return queue
	}
	return nil
}

func (mq *MessageQueue) PushMessage(queueName string, message MqMessage) error {
	quens := mq.quens[queueName]
	if quens == nil {
		return QueueHaveNoHandler
	}
	quens.PushQueueData(&message)
	return nil
}

func (mq *MessageQueue) On(queueName string, handler queuenHanlder) error {
	mq.lock.Lock()
	defer mq.lock.Unlock()
	quens := mq.quens[queueName]
	if quens != nil {
		return HandlerHasRegister
	}
	quens = NewMqQueue(handler)
	mq.quens[queueName] = quens
	return nil
}
