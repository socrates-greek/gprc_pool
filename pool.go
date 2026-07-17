// Copyright 2019 shimingyah. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// ee the License for the specific language governing permissions and
// limitations under the License.

package pool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosed is the error resulting if the pool is closed via pool.Close().
var ErrClosed = errors.New("pool is closed")

// Pool interface describes a pool implementation.
// An ideal pool is threadsafe and easy to use.
type Pool interface {
	// Get returns a new connection from the pool. Closing the connections puts
	// it back to the Pool. Closing it when the pool is destroyed or full will
	// be counted as an error. we guarantee the conn.Value() isn't nil when conn isn't nil.
	Get() (Conn, error)

	// Close closes the pool and all its connections. After Close() the pool is
	// no longer usable. You can't make concurrent calls Close and Get method.
	// It will be cause panic.
	Close() error

	// Status returns the current status of the pool.
	Status() string
}

type pool struct {
	// atomic, used to get connection random
	index uint32

	// atomic, the current physical connection of pool
	current int32

	// atomic, the using logic connection of pool
	// logic connection = physical connection * MaxConcurrentStreams
	ref int32

	// pool options
	opt Options

	// all of created physical connections
	conns []*conn

	// the server address is to create connection.
	address string

	// closed set true when Close is called.
	closed int32

	// control the atomic var current's concurrent read write.
	sync.RWMutex

	checkerCh   chan struct{}      // 健康检查控制通道
	stopChecker context.CancelFunc // 停止健康检查的函数
	healthCache []int32            // 连接健康状态缓存(原子操作: 1=健康, 0=失效)
}

// New return a connection pool.
func New(address string, option Options) (Pool, error) {
	if address == "" {
		return nil, errors.New("invalid address settings")
	}
	if option.Dial == nil {
		return nil, errors.New("invalid dial settings")
	}
	if option.MaxIdle <= 0 || option.MaxActive <= 0 || option.MaxIdle > option.MaxActive {
		return nil, errors.New("invalid maximum settings")
	}
	if option.MaxConcurrentStreams <= 0 {
		return nil, errors.New("invalid maximun settings")
	}

	p := &pool{
		index:       0,
		current:     int32(option.MaxIdle),
		ref:         0,
		opt:         option,
		conns:       make([]*conn, option.MaxActive),
		address:     address,
		closed:      0,
		healthCache: make([]int32, option.MaxActive),
	}

	//// 初始化健康状态为健康
	for i := range p.healthCache {
		atomic.StoreInt32(&p.healthCache[i], 1)
	}

	// 启动健康检查 [3,5](@ref)
	ctx, cancel := context.WithCancel(context.Background())
	p.stopChecker = cancel
	go p.startHealthChecker(ctx)

	for i := 0; i < p.opt.MaxIdle; i++ {
		c, err := p.opt.Dial(address)
		if err != nil {
			err := p.Close()
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("dial is not able to fill the pool: %s", err)
		}
		p.conns[i] = p.wrapConn(c, false)
	}
	log.Printf("new pool success: %v\n", p.Status())

	return p, nil
}

func (p *pool) incrRef() int32 {
	newRef := atomic.AddInt32(&p.ref, 1)
	if newRef == math.MaxInt32 {
		panic(fmt.Sprintf("overflow ref: %d", newRef))
	}
	return newRef
}

func (p *pool) decrRef() {
	newRef := atomic.AddInt32(&p.ref, -1)
	if newRef < 0 && atomic.LoadInt32(&p.closed) == 0 {
		panic(fmt.Sprintf("negative ref: %d", newRef))
	}
	if newRef == 0 && atomic.LoadInt32(&p.current) > int32(p.opt.MaxIdle) {
		p.Lock()
		if atomic.LoadInt32(&p.ref) == 0 {
			log.Printf("shrink pool: %d ---> %d, decrement: %d, maxActive: %d\n",
				p.current, p.opt.MaxIdle, p.current-int32(p.opt.MaxIdle), p.opt.MaxActive)
			atomic.StoreInt32(&p.current, int32(p.opt.MaxIdle))
			p.deleteFrom(p.opt.MaxIdle)
		}
		p.Unlock()
	}
}

func (p *pool) reset(index int) {
	conn := p.conns[index]
	if conn == nil {
		return
	}
	conn.reset()
	p.conns[index] = nil
}

func (p *pool) deleteFrom(begin int) {
	for i := begin; i < p.opt.MaxActive; i++ {
		p.reset(i)
	}
}

// Get see Pool interface.
func (p *pool) Get() (Conn, error) {
	// the first selected from the created connections
	nextRef := p.incrRef()
	p.RLock()
	current := atomic.LoadInt32(&p.current)
	p.RUnlock()
	if current == 0 {
		return nil, ErrClosed
	}
	if nextRef <= current*int32(p.opt.MaxConcurrentStreams) {
		next := atomic.AddUint32(&p.index, 1) % uint32(current)
		return p.conns[next], nil
	}

	// the number connection of pool is reach to max active
	if current == int32(p.opt.MaxActive) {
		// the second if reuse is true, select from pool's connections
		if p.opt.Reuse {
			next := atomic.AddUint32(&p.index, 1) % uint32(current)
			return p.conns[next], nil
		}
		// the third create one-time connection
		c, err := p.opt.Dial(p.address)
		return p.wrapConn(c, true), err
	}

	// the fourth create new connections given back to pool
	p.Lock()
	current = atomic.LoadInt32(&p.current)
	if current < int32(p.opt.MaxActive) && nextRef > current*int32(p.opt.MaxConcurrentStreams) {
		// 2 times the incremental or the remain incremental
		increment := current
		if current+increment > int32(p.opt.MaxActive) {
			increment = int32(p.opt.MaxActive) - current
		}
		var i int32
		var err error
		for i = 0; i < increment; i++ {
			c, er := p.opt.Dial(p.address)
			if er != nil {
				err = er
				break
			}
			p.reset(int(current + i))
			p.conns[current+i] = p.wrapConn(c, false)
		}
		current += i
		log.Printf("grow pool: %d ---> %d, increment: %d, maxActive: %d\n",
			p.current, current, increment, p.opt.MaxActive)
		atomic.StoreInt32(&p.current, current)
		if err != nil {
			p.Unlock()
			return nil, err
		}
	}
	p.Unlock()
	next := atomic.AddUint32(&p.index, 1) % uint32(current)
	return p.conns[next], nil
}

// Close see Pool interface.
func (p *pool) Close() error {
	atomic.StoreInt32(&p.closed, 1)
	atomic.StoreUint32(&p.index, 0)
	atomic.StoreInt32(&p.current, 0)
	atomic.StoreInt32(&p.ref, 0)
	p.deleteFrom(0)
	log.Printf("close pool success: %v\n", p.Status())

	// 触发健康检查协程退出
	if p.stopChecker != nil {
		p.stopChecker()
	}

	return nil
}

// Status see Pool interface.
func (p *pool) Status() string {
	return fmt.Sprintf("address:%s, index:%d, current:%d, ref:%d. option:%v",
		p.address, p.index, p.current, p.ref, p.opt)
}

func (p *pool) startHealthChecker(ctx context.Context) {
	ticker := time.NewTicker(p.opt.HealthCheckInterval)
	defer ticker.Stop()
	log.Printf("go pool startHealthChecker pool: %v\n", len(p.conns))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.checkConnections()
		}
	}
}

func (p *pool) checkConnections() {
	p.RLock()
	defer p.RUnlock()

	current := int(atomic.LoadInt32(&p.current))
	for i := 0; i < current; i++ {
		conn := p.conns[i]
		if conn == nil {
			continue
		}

		//log.Printf("pool startHealthChecker: %v,%v\n", time.Since(conn.lastActive), p.opt.MinCheckInterval)
		// 跳过最近检查过的连接（避免频繁检查）
		if time.Since(conn.lastActive) < p.opt.MinCheckInterval {
			//log.Printf("pool startHealthChecker: %v,%v\n", time.Since(conn.lastActive), p.opt.MinCheckInterval)
			continue
		}

		// 执行健康检查 [6](@ref)
		healthy := conn.IsHealthy()
		atomic.StoreInt32(&p.healthCache[i], boolToInt(healthy))
		//log.Printf("startHealthChecker: %v,健康情况 %v,MinCheckInterval:%v", time.Since(conn.lastActive), healthy, p.opt.MinCheckInterval)
		// 处理失效连接
		if !healthy {
			p.handleUnhealthyConn(i)
		}
	}
}

// 处理失效连接（带锁保护）
func (p *pool) handleUnhealthyConn(index int) {
	p.Lock()
	defer p.Unlock()
	// 双重检查（避免重复处理）
	if atomic.LoadInt32(&p.healthCache[index]) == 0 {
		log.Printf("removing unhealthy connection at index %d", index)
		p.reset(index)
		// 创建新连接替换 [4](@ref)
		if newConn, err := p.opt.Dial(p.address); err == nil {
			p.conns[index] = p.wrapConn(newConn, false)
			atomic.StoreInt32(&p.healthCache[index], 1)
		}
	}
}

func boolToInt(b bool) int32 {
	var i int32
	if b {
		i = 1
	} else {
		i = 0
	}
	return i
}
