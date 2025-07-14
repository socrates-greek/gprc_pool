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

package main

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"time"
)

// Conn single grpc connection inerface
type Conn interface {
	// Value return the actual grpc connection type *grpc.ClientConn.
	Value() *grpc.ClientConn

	// Close decrease the reference of grpc connection, instead of close it.
	// if the pool is full, just close it.
	Close() error
}

// Conn is wrapped grpc.ClientConn. to provide close and value method.
type conn struct {
	cc         *grpc.ClientConn
	pool       *pool
	once       bool
	lastActive time.Time // 记录最后活跃时间[6](@ref)

}

// Value see Conn interface.
func (c *conn) Value() *grpc.ClientConn {
	return c.cc
}

// Close see Conn interface.
func (c *conn) Close() error {
	c.pool.decrRef()
	if c.once {
		return c.reset()
	}
	return nil
}

func (c *conn) reset() error {
	cc := c.cc
	c.cc = nil
	c.once = false
	if cc != nil {
		return cc.Close()
	}
	return nil
}

func (p *pool) wrapConn(cc *grpc.ClientConn, once bool) *conn {
	return &conn{
		cc:         cc,
		pool:       p,
		once:       once,
		lastActive: time.Now(), // 初始化活跃时间
	}
}

// IsHealthy 检查gRPC连接的健康状态
func (c *conn) IsHealthy() bool {
	// 1. 检查底层连接是否已关闭
	if c.cc == nil {
		return false
	}
	// 2. 使用gRPC内置状态机检查[1](@ref)
	state := c.cc.GetState()
	switch state {
	case connectivity.Ready, connectivity.Idle, connectivity.Connecting:
		// 活跃或空闲状态视为健康
		c.lastActive = time.Now()
		return true
	case connectivity.TransientFailure:
		// 短暂故障：尝试执行轻量级PING检查[6](@ref)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return c.cc.WaitForStateChange(ctx, state) != true
	default:
		return false // Connecting/Shutdown状态直接返回不健康
	}
}
