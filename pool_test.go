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
	"codeup.aliyun.com/6145b2b428003bdc3daa97c8/go-simba/go-simba-proto.git/gen"
	"context"
	"flag"
	"github.com/Bifang-Bird/gprc_pool/example/pb"
	"github.com/stretchr/testify/require"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var endpoint = flag.String("endpoint", "127.0.0.1:50000", "grpc server endpoint")

func newPool(op *Options) (Pool, *pool, Options, error) {
	opt := DefaultOptions
	opt.Dial = DialTest
	if op != nil {
		opt = *op
	}
	p, err := New(*endpoint, opt)
	return p, p.(*pool), opt, err
}

func TestNew(t *testing.T) {
	p, nativePool, opt, err := newPool(nil)
	require.NoError(t, err)
	defer p.Close()

	require.EqualValues(t, 0, nativePool.index)
	require.EqualValues(t, 0, nativePool.ref)
	require.EqualValues(t, opt.MaxIdle, nativePool.current)
	require.EqualValues(t, opt.MaxActive, len(nativePool.conns))
}

func TestNew2(t *testing.T) {
	opt := DefaultOptions

	_, err := New("", opt)
	require.Error(t, err)

	opt.Dial = nil
	_, err = New("127.0.0.1:8080", opt)
	require.Error(t, err)

	opt = DefaultOptions
	opt.MaxConcurrentStreams = 0
	_, err = New("127.0.0.1:8080", opt)
	require.Error(t, err)

	opt = DefaultOptions
	opt.MaxIdle = 0
	_, err = New("127.0.0.1:8080", opt)
	require.Error(t, err)

	opt = DefaultOptions
	opt.MaxActive = 0
	_, err = New("127.0.0.1:8080", opt)
	require.Error(t, err)

	opt = DefaultOptions
	opt.MaxIdle = 2
	opt.MaxActive = 1
	_, err = New("127.0.0.1:8080", opt)
	require.Error(t, err)
}

func TestClose(t *testing.T) {
	p, nativePool, opt, err := newPool(nil)
	require.NoError(t, err)
	p.Close()

	require.EqualValues(t, 0, nativePool.index)
	require.EqualValues(t, 0, nativePool.ref)
	require.EqualValues(t, 0, nativePool.current)
	require.EqualValues(t, true, nativePool.conns[0] == nil)
	require.EqualValues(t, true, nativePool.conns[opt.MaxIdle-1] == nil)
}

func TestReset(t *testing.T) {
	p, nativePool, opt, err := newPool(nil)
	require.NoError(t, err)
	defer p.Close()

	nativePool.reset(0)
	require.EqualValues(t, true, nativePool.conns[0] == nil)
	nativePool.reset(opt.MaxIdle + 1)
	require.EqualValues(t, true, nativePool.conns[opt.MaxIdle+1] == nil)
}

func TestBasicGet(t *testing.T) {
	p, nativePool, _, err := newPool(nil)
	require.NoError(t, err)
	defer p.Close()

	conn, err := p.Get()
	require.NoError(t, err)
	require.EqualValues(t, true, conn.Value() != nil)

	require.EqualValues(t, 1, nativePool.index)
	require.EqualValues(t, 1, nativePool.ref)

	conn.Close()

	require.EqualValues(t, 1, nativePool.index)
	require.EqualValues(t, 0, nativePool.ref)
}

func TestGetAfterClose(t *testing.T) {
	p, _, _, err := newPool(nil)
	require.NoError(t, err)
	p.Close()

	_, err = p.Get()
	require.EqualError(t, err, "pool is closed")
}

func TestBasicGet2(t *testing.T) {
	opt := DefaultOptions
	opt.Dial = DialTest
	opt.MaxIdle = 1
	opt.MaxActive = 2
	opt.MaxConcurrentStreams = 2
	opt.Reuse = true

	p, nativePool, _, err := newPool(&opt)
	require.NoError(t, err)
	defer p.Close()

	conn1, err := p.Get()
	require.NoError(t, err)
	defer conn1.Close()

	conn2, err := p.Get()
	require.NoError(t, err)
	defer conn2.Close()

	require.EqualValues(t, 2, nativePool.index)
	require.EqualValues(t, 2, nativePool.ref)
	require.EqualValues(t, 1, nativePool.current)

	// create new connections push back to pool
	conn3, err := p.Get()
	require.NoError(t, err)
	defer conn3.Close()

	require.EqualValues(t, 3, nativePool.index)
	require.EqualValues(t, 3, nativePool.ref)
	require.EqualValues(t, 2, nativePool.current)

	conn4, err := p.Get()
	require.NoError(t, err)
	defer conn4.Close()

	// reuse exists connections
	conn5, err := p.Get()
	require.NoError(t, err)
	defer conn5.Close()

	nativeConn := conn5.(*conn)
	require.EqualValues(t, false, nativeConn.once)
}

func TestBasicGet3(t *testing.T) {
	opt := DefaultOptions
	opt.Dial = DialTest
	opt.MaxIdle = 1
	opt.MaxActive = 1
	opt.MaxConcurrentStreams = 1
	opt.Reuse = false

	p, _, _, err := newPool(&opt)
	require.NoError(t, err)
	defer p.Close()

	conn1, err := p.Get()
	require.NoError(t, err)
	defer conn1.Close()

	// create new connections doesn't push back to pool
	conn2, err := p.Get()
	require.NoError(t, err)
	defer conn2.Close()

	nativeConn := conn2.(*conn)
	require.EqualValues(t, true, nativeConn.once)
}

func TestConcurrentGet(t *testing.T) {
	opt := DefaultOptions
	opt.Dial = DialTest
	opt.MaxIdle = 8
	opt.MaxActive = 64
	opt.MaxConcurrentStreams = 2
	opt.Reuse = false

	p, nativePool, _, err := newPool(&opt)
	require.NoError(t, err)
	defer p.Close()

	var wg sync.WaitGroup
	wg.Add(500)

	for i := 0; i < 500; i++ {
		go func(i int) {
			conn, err := p.Get()
			require.NoError(t, err)
			require.EqualValues(t, true, conn != nil)
			conn.Close()
			wg.Done()
			t.Logf("goroutine: %v, index: %v, ref: %v, current: %v", i,
				atomic.LoadUint32(&nativePool.index),
				atomic.LoadInt32(&nativePool.ref),
				atomic.LoadInt32(&nativePool.current))
		}(i)
	}
	wg.Wait()

	require.EqualValues(t, 0, nativePool.ref)
	require.EqualValues(t, opt.MaxIdle, nativePool.current)
	require.EqualValues(t, true, nativePool.conns[0] != nil)
	require.EqualValues(t, true, nativePool.conns[opt.MaxIdle] == nil)
}

var size = 4 * 1024 * 1024

func BenchmarkPoolRPC(b *testing.B) {
	opt := DefaultOptions
	p, err := New(*endpoint, opt)
	if err != nil {
		b.Fatalf("failed to new pool: %v", err)
	}
	defer p.Close()

	testFunc := func() {
		conn, err := p.Get()
		if err != nil {
			b.Fatalf("failed to get conn: %v", err)
		}
		defer conn.Close()

		client := pb.NewEchoClient(conn.Value())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		data := make([]byte, size)
		_, err = client.Say(ctx, &pb.EchoRequest{Message: data})
		if err != nil {
			b.Fatalf("unexpected error from Say: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(tpb *testing.PB) {
		for tpb.Next() {
			testFunc()
		}
	})
}

func BenchmarkSingleRPC(b *testing.B) {
	testFunc := func() {
		cc, err := Dial(*endpoint)
		if err != nil {
			b.Fatalf("failed to create grpc conn: %v", err)
		}

		client := pb.NewEchoClient(cc)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		data := make([]byte, size)
		_, err = client.Say(ctx, &pb.EchoRequest{Message: data})
		if err != nil {
			b.Fatalf("unexpected error from Say: %v", err)
		}
	}

	b.RunParallel(func(tpb *testing.PB) {
		for tpb.Next() {
			testFunc()
		}
	})
}

func TestNewConnectionPool_Continuous(t *testing.T) {
	//// 1. 创建连接池
	//pool, err := NewConnectionPool(
	//	"cp-connect-sim.cn-dev.simbalink.cn:30000",
	//	10,            // 最大连接数
	//	5*time.Minute, // 空闲超时
	//	grpc.WithInsecure(),
	//	grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
	//)
	//if err != nil {
	//	log.Fatalf("创建连接池失败: %v", err)
	//}
	//defer pool.Close()
	//// 2. 配置重试策略
	//retryPolicy := DefaultRetryPolicy()

	options := DefaultOptions
	options.MinCheckInterval = 500 * time.Millisecond
	options.HealthCheckInterval = 1 * time.Second
	options.MaxConcurrentStreams = 150
	options.MaxActive = 50
	p, err := New("cp-connect-sim.cn-dev.simbalink.cn:30000", options)
	if err != nil {
		log.Printf("创建连接池失败: %v", err)
	}
	defer p.Close()

	log.Printf("创建连接成功: %v", err)

	//pool, err := NewConnPool(
	//	"cp-connect-sim.cn-dev.simbalink.cn:30000",
	//	1000,           // 最大连接数
	//	2*time.Minute,  // 空闲超时时间
	//	10*time.Minute, // 连接最大生命周期
	//	grpc.WithKeepaliveParams(keepalive.ClientParameters{
	//		Time:    30 * time.Second,
	//		Timeout: 10 * time.Second,
	//	}),
	//)
	//if err != nil {
	//	log.Fatalf("创建连接池失败: %v", err)
	//}

	// 3. 启动并发任务循环
	const concurrency = 1000 // 并发数量

	var wg sync.WaitGroup
	ticker := time.NewTicker(1 * time.Second) // 每秒打印一次统计信息
	defer ticker.Stop()

	// 用于统计
	var totalCalls, successCalls, failedCalls int64
	var totalLatency time.Duration

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					start := time.Now()
					//err := RetryInvoke(
					//	context.Background(),
					//	pool,
					//	retryPolicy,
					//	func(conn *grpc.ClientConn) error {
					//		req := &gen.QuerySimRequest{
					//			Type: "iccid",
					//			Data: "10000006821260360001",
					//		}
					//		resp, err := gen.NewConnectSimServiceClient(conn).QuerySimInfo(context.Background(), req)
					//		if err != nil || resp.Code != 0 {
					//			return status.Errorf(codes.Unavailable, "RPC失败: %v, resp: %v", err, resp)
					//		}
					//		//log.Println("RPC结果:", resp)
					//		return nil
					//	},
					//)

					//测试pool2-start
					conn, err := p.Get()
					if err != nil {
						log.Printf("创建连接失败: %v", err)
					}

					req := &gen.QuerySimRequest{
						Type: "iccid",
						Data: "10000006821260360001",
					}
					resp, err := gen.NewConnectSimServiceClient(conn.Value()).QuerySimInfo(context.Background(), req)
					if err != nil || resp.Code != 0 {
						log.Printf("RPC失败: %v, resp: %v", err, resp)
					}
					//测试pool2-end

					////测试pool3-start
					//conn, err := pool.Get(ctx)
					//if err != nil {
					//	log.Printf("[%d] 获取连接失败: %v", i, err)
					//	continue // 继续尝试而非退出
					//}
					//
					//req := &gen.QuerySimRequest{
					//	Type: "iccid",
					//	Data: "10000006821260360001",
					//}
					//resp, err := gen.NewConnectSimServiceClient(conn.ClientConn).QuerySimInfo(context.Background(), req)
					//if err != nil || resp.Code != 0 {
					//	log.Printf("RPC失败: %v, resp: %v", err, resp)
					//}
					//// 无论成功与否，使用完后归还连接
					//pool.Put(conn)
					////测试pool3-end

					elapsed := time.Since(start)
					atomic.AddInt64(&totalCalls, 1)
					atomic.AddInt64((*int64)(&totalLatency), int64(elapsed))
					if err == nil {
						atomic.AddInt64(&successCalls, 1)
					} else {
						atomic.AddInt64(&failedCalls, 1)
					}
				}
			}
		}(i)
	}

	// 主循环：每秒打印统计信息
	for {
		select {
		case <-ticker.C:
			calls := atomic.LoadInt64(&totalCalls)
			success := atomic.LoadInt64(&successCalls)
			failed := atomic.LoadInt64(&failedCalls)
			avgLatency := time.Duration(0)
			if calls > 0 {
				avgLatency = time.Duration(atomic.LoadInt64((*int64)(&totalLatency))/calls) * time.Nanosecond
			}
			log.Printf("[统计] 总调用: %d | 成功: %d | 失败: %d | 平均延迟: %v", calls, success, failed, avgLatency)
		case <-ctx.Done():
			return
		}
	}
}

//func TestNewConnectionPool_dynamic(t *testing.T) {
//	ctx := context.Background()
//
//	dynPool, err := grpcpool.NewDynamicPool(
//		"dev",
//		"cp-connect-sim.cn-dev.simbalink.cn:30000",
//		func(addr string) (grpcpool.Pool, error) {
//			opt := grpcpool.Options{
//				MaxIdle:              2,
//				MaxActive:            10,
//				MaxConcurrentStreams: 100,
//				HealthCheckInterval:  5 * time.Second,
//				MinCheckInterval:     1 * time.Second,
//				Reuse:                true,
//				Dial: func(addr string) (grpcpool.Conn, error) {
//					conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
//					if err != nil {
//						return nil, err
//					}
//					return &grpcpool.GrpcConn{
//						Conn: conn,
//						Addr: addr,
//					}, nil
//				},
//			}
//			return grpcpool.New(addr, opt)
//		},
//		func(p grpcpool.Pool) {
//			err := p.Close()
//			if err != nil {
//				return
//			}
//		},
//	)
//	if err != nil {
//		log.Fatalf("failed to create dynamic pool: %v", err)
//	}
//
//	kubeconfig := "" // 使用 in-cluster config
//	err = dynPool.StartEndpointWatcher(ctx, kubeconfig)
//	if err != nil {
//		log.Fatalf("failed to start watcher: %v", err)
//	}
//
//	// 模拟运行一段时间
//	log.Println("running... press Ctrl+C to exit")
//	<-ctx.Done()
//}
