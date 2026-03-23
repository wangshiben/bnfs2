package messagequeue

import "sync"

type Queue struct {
	Data []interface{}
	lock sync.RWMutex
}

func (q *Queue) Append(data interface{}) int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	i := len(q.Data)

	q.Data = append(q.Data, data)
	return i
}
func (q *Queue) Marshal() []interface{} {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return q.Data
}
func (q *Queue) Pop() interface{} {
	var res interface{}
	if len(q.Data) > 1 {
		res = q.Data[0]
		q.Data = q.Data[1:]
	} else if len(q.Data) == 1 {
		res = q.Data[0]
		q.Data = make([]interface{}, 0)
	} else {
		return nil
	}
	return res

}

func NewQueue() *Queue {
	return &Queue{
		Data: make([]interface{}, 0),
		lock: sync.RWMutex{},
	}
}
