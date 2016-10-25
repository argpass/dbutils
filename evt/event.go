package evt

import "reflect"

type Event interface {
}

type EventHandler func(e Event) (result interface{})

type EventType reflect.Type

var registry = RegistryType{}
type RegistryType map[EventType] []EventHandler

// Subscribe connects a `EventHandler` with to a `EventType`
func (re RegistryType) Subscribe(event Event, handler EventHandler) {
	tp := EventType(reflect.ValueOf(event).Type())
	if s, ok := re[tp]; ok {
		s = append(s, handler)
	}else{
		re[tp]=[]EventHandler{handler}
	}
}

func (re RegistryType) Handlers(event Event)(handlers []EventHandler) {
	tp := EventType(reflect.ValueOf(event).Type())
	var ok = false
	handlers, ok = re[tp]
	if !ok {
		return nil
	}
	return handlers
}

// Subscribe an `Event` to handle with fn
func Subscribe(event Event, fn EventHandler) {
	registry.Subscribe(event, fn)
}

// SynSend sends event and get results from connected handlers
func SynSend(event Event)(results []interface{}){
	handlers := registry.Handlers(event)
	for i, fn := range handlers {
		if results == nil {
			results = make([]interface{}, len(handlers))
		}
		results[i] = fn(event)
	}
	return results
}

