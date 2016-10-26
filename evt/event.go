package evt

import (
	"reflect"
	"sync"
)

type Event interface {
}

type EventHandler func(e Event) (result interface{})

type EventType reflect.Type

var registry = registryType{}

var registryLock = &sync.RWMutex{}

type registryType map[EventType] []EventHandler

// Subscribe connects a `EventHandler` with to a `EventType`
func (re registryType) Subscribe(event Event, handler EventHandler) {
	registryLock.Lock()
	defer func(){
		registryLock.Unlock()
	}()

	tp := EventType(reflect.ValueOf(event).Type())
	if s, ok := re[tp]; ok {
		s = append(s, handler)
	}else{
		re[tp]=[]EventHandler{handler}
	}
}

// NotifyAll notifies handlers with the `event`
func (re registryType) NotifyAll(event Event)(results []interface{}) {
	var handlers []EventHandler
	// never to change registry when notifying
	registryLock.RLock()
	defer func(){
		registryLock.RUnlock()
	}()

	// get all handlers connected with the `event`
	var ok = false
	tp := EventType(reflect.ValueOf(event).Type())
	handlers, ok = re[tp]
	if !ok {
		return results
	}

	// call all handlers on `event`, make results slice
	for i, fn := range handlers {
		if results == nil {
			results = make([]interface{}, len(handlers))
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

