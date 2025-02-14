package server

import (
	"encoding/json"
	"github.com/meiti-x/go-msg-broker/queue"
	"net"
	"sync"
)

type Topic struct {
	Name         string
	MQ           queue.IMessageQueue
	Subs         map[string]net.Conn
	subMu        sync.Mutex
	BufferedChan chan bool
}

func NewTopic(name string) *Topic {
	t := &Topic{
		Name:         name,
		MQ:           queue.NewMessageQueue(),
		Subs:         make(map[string]net.Conn),
		BufferedChan: make(chan bool, 1000),
	}

	go t.Listen()

	return t
}

func (t *Topic) AddSubscriber(conn net.Conn) {
	t.subMu.Lock()
	defer t.subMu.Unlock()
	t.Subs[conn.RemoteAddr().String()] = conn
}

func (t *Topic) RemoveSubscriber(conn net.Conn) {
	t.subMu.Lock()
	defer t.subMu.Unlock()

	delete(t.Subs, conn.RemoteAddr().String())
}

func (t *Topic) GetMessageQueue() *queue.MessageQueue {
	if mq, ok := t.MQ.(*queue.MessageQueue); ok {
		return mq
	}
	return nil
}

func (t *Topic) IsSubscribed(conn net.Conn) bool {
	t.subMu.Lock()
	defer t.subMu.Unlock()

	_, ok := t.Subs[conn.RemoteAddr().String()]
	return ok
}

func (t *Topic) BroadCast(msg *queue.Message) {
	t.subMu.Lock()
	defer t.subMu.Unlock()

	r := map[string]any{
		"action": "deliver",
		"message": map[string]any{
			"message_id": msg.ID,
			"topic":      t.Name,
			"content":    msg.Content,
			"priority":   msg.Priority,
		},
	}

	for _, conn := range t.Subs {
		go func(c net.Conn) {
			encoder := json.NewEncoder(c)
			_ = encoder.Encode((r))
		}(conn)
	}
}

func (t *Topic) Listen() {
	for {
		select {
		case s := <-t.BufferedChan:
			if s {
				msg := t.GetMessageQueue().RemoveMessage()
				t.BroadCast(msg)
			} else {
				return
			}
		}
	}
}
