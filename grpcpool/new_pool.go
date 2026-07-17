package grpcpool

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"math/rand"
	"sync"
	"time"
)

// ========================= 连接池实现 =========================
type ConnectionPool struct {
	target      string
	dialOptions []grpc.DialOption
	maxSize     int
	idleTimeout time.Duration

	mu          sync.Mutex
	connections chan *PooledConnection
	closed      bool
}

type PooledConnection struct {
	*grpc.ClientConn
	lastUsed time.Time
}

func NewConnectionPool(target string, maxSize int, idleTimeout time.Duration, opts ...grpc.DialOption) (*ConnectionPool, error) {
	if maxSize <= 0 {
		return nil, errors.New("invalid maxSize")
	}

	pool := &ConnectionPool{
		target:      target,
		maxSize:     maxSize,
		idleTimeout: idleTimeout,
		connections: make(chan *PooledConnection, maxSize),
		dialOptions: opts,
	}

	// 预热连接池
	for i := 0; i < 2; i++ { // 初始创建2个连接
		conn, err := pool.createConnection()
		if err != nil {
			return nil, err
		}
		pool.connections <- conn
	}

	go pool.healthCheckLoop() // 启动健康检查
	return pool, nil
}

func (p *ConnectionPool) createConnection() (*PooledConnection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, p.target, p.dialOptions...)
	if err != nil {
		return nil, err
	}

	return &PooledConnection{
		ClientConn: conn,
		lastUsed:   time.Now(),
	}, nil
}

func (p *ConnectionPool) Get() (*PooledConnection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, errors.New("pool is closed")
	}

	// 尝试获取空闲连接
	select {
	case conn := <-p.connections:
		if conn.IsIdleExpired(p.idleTimeout) {
			conn.Close()
			return p.createConnection() // 创建新连接替代过期连接
		}
		return conn, nil
	default:
		// 无空闲连接时创建新连接
		return p.createConnection()
	}
}

func (p *ConnectionPool) Put(conn *PooledConnection) {
	if conn == nil || conn.GetState() == connectivity.Shutdown {
		return
	}

	conn.lastUsed = time.Now() // 更新最后使用时间

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		conn.Close()
		return
	}

	select {
	case p.connections <- conn: // 放回连接池
	default:
		conn.Close() // 连接池已满，直接关闭
	}
}

func (p *ConnectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	p.closed = true
	close(p.connections)

	for conn := range p.connections {
		conn.Close()
	}
}

func (c *PooledConnection) IsIdleExpired(timeout time.Duration) bool {
	return time.Since(c.lastUsed) > timeout
}

// 后台健康检查协程
func (p *ConnectionPool) healthCheckLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}

		// 检查所有空闲连接
		for i := 0; i < len(p.connections); i++ {
			select {
			case conn := <-p.connections:
				if conn.IsIdleExpired(p.idleTimeout) || conn.GetState() != connectivity.Ready {
					conn.Close()
				} else {
					p.connections <- conn
				}
			default:
				break
			}
		}
		p.mu.Unlock()
	}
}

// ========================= 重试策略实现 =========================
type RetryPolicy struct {
	MaxAttempts       int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
	RetryableCodes    []codes.Code
}

func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts:       3,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        3 * time.Second,
		BackoffMultiplier: 2,
		RetryableCodes: []codes.Code{
			codes.Unavailable,
			codes.DeadlineExceeded,
			codes.ResourceExhausted,
		},
	}
}

func (p *RetryPolicy) IsRetryable(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}

	for _, code := range p.RetryableCodes {
		if s.Code() == code {
			return true
		}
	}
	return false
}

// 带重试的RPC调用封装
func RetryInvoke(
	ctx context.Context,
	pool *ConnectionPool,
	policy *RetryPolicy,
	invokeFunc func(conn *grpc.ClientConn) error,
) error {
	var lastErr error

	for attempt := 0; attempt < policy.MaxAttempts; attempt++ {
		conn, err := pool.Get()
		if err != nil {
			return err
		}

		err = invokeFunc(conn.ClientConn)
		pool.Put(conn)

		if err == nil {
			return nil // 成功返回
		}

		if !policy.IsRetryable(err) {
			return err // 不可重试错误
		}

		lastErr = err
		backoff := calculateBackoff(policy, attempt)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return lastErr
}

// 指数退避计算
func calculateBackoff(policy *RetryPolicy, attempt int) time.Duration {
	backoff := policy.InitialBackoff
	for i := 0; i < attempt; i++ {
		backoff = time.Duration(float64(backoff) * policy.BackoffMultiplier)
		if backoff > policy.MaxBackoff {
			backoff = policy.MaxBackoff
		}
	}

	// 添加随机抖动 (30% 范围)
	factor := 0.5 + rand.Float64()*0.5 // [0.5, 1.0)
	jitter := time.Duration(float64(backoff) * 0.3 * factor)
	return backoff + jitter
}
