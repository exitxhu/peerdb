package utils

import (
	"context"
	"fmt"
	"log/slog"

	lua "github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/gluaflatbuffers"
	"github.com/PeerDB-io/gluajson"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/pua"
	"github.com/PeerDB-io/peer-flow/shared"
)

func LVAsReadOnlyBytes(ls *lua.LState, v lua.LValue) ([]byte, error) {
	str, err := LVAsStringOrNil(ls, v)
	if err != nil {
		return nil, err
	} else if str == "" {
		return nil, nil
	} else {
		return shared.UnsafeFastStringToReadOnlyBytes(str), nil
	}
}

func LVAsStringOrNil(ls *lua.LState, v lua.LValue) (string, error) {
	if lstr, ok := v.(lua.LString); ok {
		return string(lstr), nil
	} else if v == lua.LNil {
		return "", nil
	} else {
		return "", fmt.Errorf("invalid bytes, must be nil or string: %s", v)
	}
}

func LoadScript(ctx context.Context, script string, printfn lua.LGFunction) (*lua.LState, error) {
	ls := lua.NewState(lua.Options{SkipOpenLibs: true})
	ls.SetContext(ctx)
	for _, pair := range []struct {
		f lua.LGFunction
		n string
	}{
		{lua.OpenPackage, lua.LoadLibName}, // Must be first
		{lua.OpenBase, lua.BaseLibName},
		{lua.OpenTable, lua.TabLibName},
		{lua.OpenString, lua.StringLibName},
		{lua.OpenMath, lua.MathLibName},
	} {
		ls.Push(ls.NewFunction(pair.f))
		ls.Push(lua.LString(pair.n))
		err := ls.PCall(1, 0, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize Lua runtime: %w", err)
		}
	}
	ls.PreloadModule("flatbuffers", gluaflatbuffers.Loader)
	pua.RegisterTypes(ls)
	ls.Env.RawSetString("print", ls.NewFunction(printfn))
	if script != "" {
		err := ls.GPCall(pua.LoadPeerdbScript, lua.LString(script))
		if err != nil {
			return nil, fmt.Errorf("error loading script %s: %w", script, err)
		}
		err = ls.PCall(0, 0, nil)
		if err != nil {
			return nil, fmt.Errorf("error executing script %s: %w", script, err)
		}
	}
	return ls, nil
}

func DefaultOnRecord(ls *lua.LState) int {
	ud, record := pua.LuaRecord.Check(ls, 1)
	if _, ok := record.(*model.RelationRecord[model.RecordItems]); ok {
		return 0
	}
	ls.Push(ls.NewFunction(gluajson.LuaJsonEncode))
	ls.Push(ud)
	ls.Call(1, 1)
	return 1
}

type LPoolMessage[T any] struct {
	f   func(*lua.LState) T
	ret chan<- T
}
type LPool[T any] struct {
	messages chan LPoolMessage[T]
	returns  chan<- (<-chan T)
	wait     <-chan struct{}
	cons     func() (*lua.LState, error)
	maxSize  int
	size     int
	closed   bool
}

func LuaPool[T any](cons func() (*lua.LState, error), merge func(T)) (*LPool[T], error) {
	maxSize := 4 // TODO env variable
	returns := make(chan (<-chan T), maxSize)
	wait := make(chan struct{})
	messages := make(chan LPoolMessage[T])
	go func() {
		for ret := range returns {
			for val := range ret {
				merge(val)
			}
		}
		close(wait)
	}()

	pool := &LPool[T]{
		messages: messages,
		returns:  returns,
		wait:     wait,
		cons:     cons,
		maxSize:  maxSize,
		size:     0,
		closed:   false,
	}
	if err := pool.Spawn(); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func (pool *LPool[T]) Spawn() error {
	ls, err := pool.cons()
	if err != nil {
		return err
	}
	pool.size += 1
	go func() {
		// log pool size
		slog.Info("[goroutine] count (pool size)", "size", pool.size)
		defer ls.Close()
		for message := range pool.messages {
			message.ret <- message.f(ls)
			close(message.ret)
		}
	}()
	slog.Info("Lua pool size", "size", pool.size)
	return nil
}

func (pool *LPool[T]) Close() {
	if !pool.closed {
		close(pool.returns)
		close(pool.messages)
		pool.closed = true
	}
}

func (pool *LPool[T]) Run(f func(*lua.LState) T) {
	ret := make(chan T, 1)
	pool.returns <- ret
	msg := LPoolMessage[T]{f: f, ret: ret}
	if pool.size < pool.maxSize {
		select {
		case pool.messages <- msg:
			return
		default:
			_ = pool.Spawn()
		}
	}
	pool.messages <- msg
}

func (pool *LPool[T]) Wait(ctx context.Context) error {
	pool.Close()
	select {
	case <-pool.wait:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
