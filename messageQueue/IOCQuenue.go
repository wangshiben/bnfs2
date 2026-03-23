package messagequeue

import (
	"sync"
)

type IOCQueue struct {
	dataList []Indexed
	rwLock   sync.RWMutex
}
type Indexed interface {
	CurrentIndex() int
	NextItem() Indexed
}

func (q *IOCQueue) GetList() []Indexed {
	q.rwLock.RLock()
	defer q.rwLock.RUnlock()
	return q.dataList
}
func (q *IOCQueue) IndexOf(index int) Indexed {
	list := q.GetList()
	if len(list) == 0 {
		return nil
	}
	item := list[0]
	for item != nil {
		if item.CurrentIndex() == index {
			return item
		}
		item = item.NextItem()

	}
	return nil
}
func AddIOCQueueData(Name string, data Indexed) {
	queue := MQ.GetIOCData(Name)
	if queue == nil {
		queue = &IOCQueue{
			dataList: []Indexed{data},
			rwLock:   sync.RWMutex{},
		}
		MQ.AddIOCData(Name, queue)
		return
	}
	queues := queue.(*IOCQueue)
	queues.rwLock.Lock()
	defer queues.rwLock.Unlock()
	queues.dataList = append(queues.dataList, data)
	MQ.AddIOCData(Name, queues)
}

func GetIOCQueue(name string) *IOCQueue {
	res := MQ.GetIOCData(name)
	if res == nil {
		return nil
	}
	return res.(*IOCQueue)
}
