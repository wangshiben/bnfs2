package messagequeue

import (
	"sync"
	"time"
)

type queuenHanlder func(data *MqMessage)

type MqQueue struct {
	header       *queueItem      // 待处理队列头
	tail         *queueItem      // 待处理队列尾
	lock         sync.Mutex      // 锁
	handler      queuenHanlder   // 处理函数
	queueChannel chan *MqMessage // 队列通道
}
type queueItem struct {
	next *queueItem
	//数据
	data *MqMessage
}

func (q *MqQueue) PushQueueData(data *MqMessage) {
	q.queueChannel <- data
}

func (q *MqQueue) addQueueData(data *MqMessage) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.header == nil { // 头处理
		q.header = &queueItem{data: data, next: nil}
		q.tail = q.header
		return
	}
	newTail := &queueItem{data: data, next: nil}
	q.tail.next = newTail
	q.tail = newTail
}
func (q *MqQueue) handleQueueData() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				// logger.Errorf("panic: %v", err)
			}
		}()

		for {
			// 1. 从链表头部取出一个任务（需要加锁）
			var msg *MqMessage
			q.lock.Lock()
			if q.header != nil {
				msg = q.header.data
				q.header = q.header.next
				if q.header == nil {
					q.tail = nil
				}
			}
			q.lock.Unlock()
			// 2. 如果有消息，交给 handler 处理
			if msg != nil {

				if q.handler != nil {
					q.handler(msg)
				}
			} else {
				// 队列为空，短暂休眠，避免忙等
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
}
func (q *MqQueue) start() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				// logger.Errorf("panic: %v", err)
			}
		}()
		for data := range q.queueChannel {
			if data.flag == -1 {
				return
			}
			q.addQueueData(data)

		}
	}()
}
func (q *MqQueue) close() {
	data := &MqMessage{flag: -1}
	q.queueChannel <- data
}
func NewMqQueue(handler queuenHanlder) *MqQueue {
	res := &MqQueue{
		handler:      handler,
		queueChannel: make(chan *MqMessage),
		lock:         sync.Mutex{},
	}
	res.start()
	res.handleQueueData()
	return res
}
