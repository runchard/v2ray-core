package protocol

import (
	"sync"
	"time"
)

type ServerList struct {
	sync.RWMutex
	servers []*ServerSpec
}

func NewServerList() *ServerList {
	return &ServerList{}
}

func (sl *ServerList) AddServer(server *ServerSpec) {
	sl.Lock()
	defer sl.Unlock()

	sl.servers = append(sl.servers, server)
}

func (sl *ServerList) Size() uint32 {
	sl.RLock()
	defer sl.RUnlock()

	return uint32(len(sl.servers))
}

func (sl *ServerList) GetServer(idx uint32) *ServerSpec {
	sl.Lock()
	defer sl.Unlock()

	for {
		if idx >= uint32(len(sl.servers)) {
			return nil
		}

		server := sl.servers[idx]
		if !server.IsValid() {
			sl.removeServer(idx)
			continue
		}

		return server
	}
}

func (sl *ServerList) removeServer(idx uint32) {
	n := len(sl.servers)
	sl.servers[idx] = sl.servers[n-1]
	sl.servers = sl.servers[:n-1]
}

type ServerPicker interface {
	PickServer() *ServerSpec
	UpdateStat(uint32)
}

type RoundRobinServerPicker struct {
	sync.Mutex
	serverlist *ServerList
	nextIndex  uint32
}

func NewRoundRobinServerPicker(serverlist *ServerList) *RoundRobinServerPicker {
	return &RoundRobinServerPicker{
		serverlist: serverlist,
		nextIndex:  0,
	}
}

func (p *RoundRobinServerPicker) PickServer() *ServerSpec {
	p.Lock()
	defer p.Unlock()

	next := p.nextIndex
	server := p.serverlist.GetServer(next)
	if server == nil {
		next = 0
		server = p.serverlist.GetServer(0)
	}
	next++
	if next >= p.serverlist.Size() {
		next = 0
	}
	p.nextIndex = next

	return server
}
func (p *RoundRobinServerPicker) UpdateStat(latency_ms uint32) {
}

// Use a haproxy style server picker by default use 0th server in serverlist
type FirstServerPicker struct {
	sync.Mutex
	serverlist  *ServerList
	latencyChan chan uint32
	serverChan  chan *ServerSpec
}

func NewFirstServerPicker(serverlist *ServerList) *FirstServerPicker {
	picker := FirstServerPicker{
		serverlist:  serverlist,
		latencyChan: make(chan uint32, 8),
		serverChan:  make(chan *ServerSpec),
	}
	go func() {
		var index uint32 = 0
		for latency_ms := range picker.latencyChan {
			if latency_ms == 0 { // reset to 0 signal
				index = 0
			} else if latency_ms > 5000 { // bad latency
				index = (index + 1) % picker.serverlist.Size()
				if index == 0 {
					time.AfterFunc(5*time.Minute, func() {
						picker.latencyChan <- 0
					})
				}
			}
		}
		go func() {
			for {
				picker.serverChan <- picker.serverlist.GetServer(index)
			}
		}()
	}()

	return &picker
}

func (p *FirstServerPicker) UpdateStat(latency_ms uint32) {
	p.latencyChan <- latency_ms
}

func (p *FirstServerPicker) PickServer() *ServerSpec {

	return <-p.serverChan
}
