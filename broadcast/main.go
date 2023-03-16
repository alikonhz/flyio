package main

import (
	"context"
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"go.uber.org/zap"
	"sort"
	"sync"
)

type server struct {
	node *maelstrom.Node

	msgMu        sync.RWMutex
	msg          map[int]struct{}
	broadcastMap map[string]chan *broadcastMsg

	topMu    sync.RWMutex
	topology []string

	logger *zap.Logger
	ctx    context.Context
}

func main() {

	n := maelstrom.NewNode()

	rawJSON := []byte(`{
	  "level": "debug",
	  "encoding": "json",
	  "outputPaths": ["stderr"],
	  "errorOutputPaths": ["stderr"],
	  "encoderConfig": {
	    "messageKey": "message",
	    "levelKey": "level",
	    "levelEncoder": "lowercase",
		"timeKey": "ts",
	    "timeEncoder": "ISO8601"
	  }
	}`)

	var cfg zap.Config
	if err := json.Unmarshal(rawJSON, &cfg); err != nil {
		panic(err)
	}

	logger := zap.Must(cfg.Build())
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &server{
		msg:          map[int]struct{}{},
		broadcastMap: make(map[string]chan *broadcastMsg),
		node:         n,
		topology:     []string{},
		ctx:          ctx,
		logger:       logger,
	}

	n.Handle("broadcast", s.handleBroadcast)
	n.Handle("read", s.handleRead)
	n.Handle("topology", s.handleTopology)

	if err := n.Run(); err != nil {
		s.logger.Error("node run error", zap.Error(err))
	}

	for _, dest := range s.topology {
		close(s.broadcastMap[dest])
	}

}

func (s *server) handleTopology(msg maelstrom.Message) error {
	err := s.updateTopology(msg)
	if err != nil {
		s.logger.Error("handleTopology", zap.Error(err))
		return err
	}

	resp := make(map[string]any)
	resp["type"] = "topology_ok"

	return s.node.Reply(msg, resp)
}

func (s *server) updateTopology(msg maelstrom.Message) error {
	body, err := readBody(msg.Body)
	if err != nil {
		return err
	}

	t, ok := body["topology"]
	if !ok {
		return fmt.Errorf("topology not found in message")
	}

	topologyMap, ok := t.(map[string]any)
	if !ok {
		return fmt.Errorf("topology is not a map[string]any. it's %T", t)
	}

	nodes, ok := topologyMap[s.node.ID()]
	if !ok {
		s.logger.Warn("no topology for node", zap.String("nodeid", s.node.ID()))
		return nil
	}

	nodesSlice, ok := nodes.([]any)
	if !ok {
		return fmt.Errorf("topology for node %s is not []any. it's %T", s.node.ID(), nodes)
	}

	s.topMu.Lock()
	defer s.topMu.Unlock()
	if len(nodesSlice) == 0 {
		s.topology = []string{}
		return nil
	}

	newTopology := make([]string, len(nodesSlice))
	for i, val := range nodesSlice {
		dest := fmt.Sprint(val)
		newTopology[i] = dest
		destChan := make(chan *broadcastMsg, 1024)
		s.broadcastMap[dest] = destChan
		go s.readDestChan(dest, destChan)
	}

	s.topology = newTopology
	s.logger.Debug("topology created", zap.String("nodeid", s.node.ID()),
		zap.String("topology", fmt.Sprintf("%+v", newTopology)))

	return nil
}

func (s *server) readDestChan(dest string, destChan chan *broadcastMsg) {
	s.logger.Info("readDestChan: starting", zap.String("dest", dest))

chanLoop:
	for {
		select {
		case msg, ok := <-destChan:
			s.logger.Info("readDestChan: got msg", zap.String("dest", dest),
				zap.Int("msg", msg.msg))
			if !ok {
				break chanLoop
			}
			for {
				if err := s.sendMsg(msg.payload, dest); err != nil {
					s.logger.Error("send to dest failed", zap.String("nodeid", s.node.ID()),
						zap.String("dest", dest),
						zap.Int("msg", msg.msg),
						zap.Error(err))
					continue
				}
				break
			}
			s.logger.Info("readDestChan: sent to dest", zap.String("dest", dest), zap.Int("msg", msg.msg))
		}
	}
}

func (s *server) handleRead(msg maelstrom.Message) error {
	resp := make(map[string]any)
	resp["type"] = "read_ok"

	s.msgMu.RLock()
	defer s.msgMu.RUnlock()

	respMsg := make([]int, len(s.msg))
	i := 0
	for key, _ := range s.msg {
		respMsg[i] = key
		i++
	}

	sort.Sort(sort.IntSlice(respMsg))

	resp["messages"] = respMsg

	return s.node.Reply(msg, resp)
}

type broadcastMsg struct {
	payload map[string]any
	msg     int
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	s.logger.Info("broadcast", zap.String("nodeid", s.node.ID()), zap.String("from", msg.Src))
	body, err := readBody(msg.Body)
	if err != nil {
		return err
	}

	message, ok := body["message"]
	if !ok {
		s.logger.Warn("handleBroadcast: no message read", zap.String("nodeid", s.node.ID()))
		return nil
	}

	msgF, ok := message.(float64)
	if !ok {
		return fmt.Errorf("message '%v' of type '%T' is not a float64", message, message)
	}

	msgI := int(msgF)
	s.logger.Info("broadcast msg", zap.String("nodeid", s.node.ID()), zap.Int("msg", msgI))

	msgPayload := broadcastMsg{msg: msgI, payload: body}

	s.saveAndBroadcastMessage(&msgPayload)

	return s.replyBroadcastOk(msg)
}

func (s *server) saveAndBroadcastMessage(msgPayload *broadcastMsg) {

	msg := msgPayload.msg
	s.msgMu.Lock()
	_, msgExists := s.msg[msg]
	if !msgExists {
		s.msg[msg] = struct{}{}
		go s.broadcastMsg(msgPayload)
	}
	s.msgMu.Unlock()
}

func (s *server) sendMsg(msgBody map[string]any, dest string) error {
	return s.node.RPC(dest, msgBody, func(m maelstrom.Message) error {
		return nil
	})
}

func (s *server) broadcastMsg(msgPayload *broadcastMsg) {
	s.logger.Info("broadcastMsg", zap.String("nodeid", s.node.ID()), zap.Int("msg", msgPayload.msg))

	s.logger.Debug("topology", zap.String("nodeid", s.node.ID()),
		zap.String("topology", fmt.Sprintf("%+v", s.topology)))

	for _, dest := range s.topology {
		go s.broadcastMsgToDest(msgPayload, dest)
	}
}

func (s *server) broadcastMsgToDest(msg *broadcastMsg, dest string) {

	destChan, ok := s.broadcastMap[dest]
	if !ok {
		return
	}

	destChan <- msg
}

func (s *server) replyBroadcastOk(req maelstrom.Message) error {
	resp := map[string]any{
		"type": "broadcast_ok",
	}

	return s.node.Reply(req, resp)
}

func readBody(data json.RawMessage) (map[string]any, error) {
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, err
	}

	return body, nil
}
