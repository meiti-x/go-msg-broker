package queue

import (
	"container/heap"

	"github.com/google/uuid"
)

type IMessageQueue interface {
	heap.Interface
}

// len,push, pop,
type Message struct {
	ID       uuid.UUID
	Content  string
	Priority int
	Index    int
}

type MessageQueue []*Message

func (mq *MessageQueue) Push(x any) {
	m := x.(*Message)
	m.Index = len(*mq)
	*mq = append(*mq, m)
}
func (mq *MessageQueue) Pop() any {
	last_queue := *mq
	last_count := len(last_queue)

	message := last_queue[last_count-1]
	message.Index = -1
	*mq = last_queue[0 : last_count-1]

	return message

}
func (mq *MessageQueue) Len() int {
	return len(*mq)
}
func (mq *MessageQueue) Less(i, j int) bool {
	return (*mq)[i].Priority < (*mq)[j].Priority
}
func (mq *MessageQueue) Swap(i, j int) {
	(*mq)[i], (*mq)[j] = (*mq)[j], (*mq)[i]
	(*mq)[i].Index = i
	(*mq)[j].Index = j
}

func (mq *MessageQueue) AddMessage(text string, priority int) *Message {
	id, _ := uuid.NewRandom()
	m := &Message{
		ID:       id,
		Content:  text,
		Priority: priority,
	}
	heap.Push(mq, m)
	return m
}

func (mq *MessageQueue) RemoveMessage() *Message {
	if mq.Len() == 0 {
		return nil
	}

	return heap.Pop(mq).(*Message)
}

func NewMessageQueue() IMessageQueue {
	mq := &MessageQueue{}
	heap.Init(mq)

	return mq
}
