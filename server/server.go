package server

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

type Server struct {
	Addr     string
	Topics   map[string]*Topic
	Clients  map[string]net.Conn
	MU       sync.Mutex
	Signal   chan bool
	Listener net.Listener
}

type Subs struct {
	Conn   net.Conn
	Topics map[string]bool
}

func NewServer(address string) *Server {
	return &Server{
		Addr:    address,
		Topics:  make(map[string]*Topic),
		Clients: make(map[string]net.Conn),
		Signal:  make(chan bool),
	}
}

func (s *Server) Run() error {
	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	s.Listener = listener
	return s.ListenAndServe()
}

func (s *Server) ListenAndServe() error {

	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			select {
			case <-s.Signal:
				return nil
			default:
				return fmt.Errorf("failed to accept connection: %w", err)
			}
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) Stop() {
	close(s.Signal)
	_ = s.Listener.Close()
}

func (s *Server) GetTopic(topicName string) (*Topic, bool) {
	s.MU.Lock()
	defer s.MU.Unlock()
	if topic, exists := s.Topics[topicName]; exists {
		return topic, true
	}
	newTopic := NewTopic(topicName)
	s.Topics[topicName] = newTopic
	return newTopic, false
}

func (s *Server) Subscribe(req map[string]any, conn net.Conn) map[string]any {
	if topicName, ok := req["topic"].(string); ok && topicName != "" {
		topic, _ := s.GetTopic(topicName)
		topic.AddSubscriber(conn)
		return map[string]any{"status": "ok"}
	}
	return map[string]any{"error": "invalid topic"}
}

func (s *Server) UnSubscribe(req map[string]any, conn net.Conn) map[string]any {
	if topicName, ok := req["topic"].(string); ok && topicName != "" {
		topic, _ := s.GetTopic(topicName)
		topic.RemoveSubscriber(conn)
		return map[string]any{"status": "ok"}
	}
	return map[string]any{"error": "invalid topic"}
}

func (s *Server) PublishMessage(request map[string]any) map[string]any {
	message, ok := request["message"].(map[string]any)
	if !ok || message == nil {
		return map[string]any{"error": "missing message"}
	}

	content, contentOk := message["content"].(string)
	topicName, topicOk := message["topic"].(string)
	priority, priorityOk := message["priority"].(float64)

	if !contentOk || !topicOk || !priorityOk {
		return map[string]any{"error": "invalid message fields"}
	}

	topic, _ := s.GetTopic(topicName)
	_ = topic.GetMessageQueue().AddMessage(content, int(priority))
	topic.BufferedChan <- true

	return map[string]any{"status": "ok"}
}

func (s *Server) GetClientConnections() []net.Conn {
	s.MU.Lock()
	defer s.MU.Unlock()

	connections := make([]net.Conn, 0, len(s.Clients))
	for _, conn := range s.Clients {
		connections = append(connections, conn)
	}
	return connections
}

func (s *Server) handleConnection(conn net.Conn) {
	s.addClient(conn)
	defer s.removeClient(conn)

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	for {
		var request map[string]any
		if err := decoder.Decode(&request); err != nil {
			continue
		}

		response := s.routeRequest(request, conn)
		if response != nil {
			_ = encoder.Encode(response)
		}
	}
}

func (s *Server) addClient(conn net.Conn) {
	s.MU.Lock()
	defer s.MU.Unlock()
	s.Clients[conn.RemoteAddr().String()] = conn
}

func (s *Server) removeClient(conn net.Conn) {
	s.MU.Lock()
	defer s.MU.Unlock()
	delete(s.Clients, conn.RemoteAddr().String())
	_ = conn.Close()
}

func (s *Server) routeRequest(request map[string]any, conn net.Conn) map[string]any {
	switch request["action"] {
	case "subscribe":
		return s.Subscribe(request, conn)
	case "unsubscribe":
		return s.UnSubscribe(request, conn)
	case "publish":
		return s.PublishMessage(request)
	case "close_connection":
		return nil
	case "shutdown":
		s.Stop()
		return nil
	default:
		return map[string]any{"status": "error", "message": "unknown action"}
	}
}
