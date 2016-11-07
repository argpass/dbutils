package evt

import (
	"reflect"
	"sync"
)

type Event interface {
}

type EventHandler func(e Event) (result interface{})

type EventType reflect.Type

var registry = newRegistryDB()

type registryDB struct {
	sync.RWMutex
	registryMap map[EventType][]EventHandler
}

func newRegistryDB() *registryDB {
	p := &registryDB{}
	p.registryMap = map[EventType][]EventHandler{}
	return p
}

// Subscribe connects a `EventHandler` with to a `EventType`
func (re *registryDB) Subscribe(event Event, handler EventHandler) {
	re.Lock()
	defer re.Unlock()

	tp := EventType(reflect.ValueOf(event).Type())
	if s, ok := re.registryMap[tp]; ok {
		s = append(s, handler)
	}else{
		re.registryMap[tp]=[]EventHandler{handler}
	}
}

// NotifyAll notifies handlers with the `event`
func (re *registryDB) NotifyAll(event Event)(results []interface{}) {
	re.RLock()
	defer re.RUnlock()

	// get all handlers connected with the `event`
	var ok = false
	tp := EventType(reflect.ValueOf(event).Type())
	_, ok = re.registryMap[tp]
	if !ok {
		return results
	}

	// call all handlers on `event`, make results slice
	for i, fn := range re.registryMap[tp] {
		if results == nil {
			results = make([]interface{}, len(re.registryMap))
		}
		results[i] = fn(event)
	}
	return results
}

// Subscribe an `Event` to handle with fn
func Subscribe(event Event, fn EventHandler) {
	registry.Subscribe(event, fn)
}

// SynSend sends event and get results from connected handlers
func SynSend(event Event)([]interface{}){
	return registry.NotifyAll(event)
}

