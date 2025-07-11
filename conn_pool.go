package pool

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ====================== 连接对象封装 ======================
type Connect struct {
	*grpc.ClientConn
	createdAt   time.Time
	lastUsed    time.Time
	lastChecked time.Time // 用于缓存健康状态
}

func (c *Connect) IsExpired(maxLifetime time.Duration) bool {
	return time.Since(c.createdAt) > maxLifetime
}

func (c *Connect) ShouldCheckHealth(idleTimeout time.Duration) bool {
	return time.Since(c.lastChecked) > idleTimeout/2
}

func (c *Connect) Validate(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client := healthpb.NewHealthClient(c.ClientConn)
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || resp.Status != healthpb.HealthCheckResponse_SERVING {
		return errors.New("connection unhealthy")
	}
	c.lastChecked = time.Now()
	return nil
}

// ====================== 连接池核心实现 ======================
type ConnPool struct {
	target      string
	maxSize     int
	idleTimeout time.Duration
	maxLifetime time.Duration
	dialOptions []grpc.DialOption

	mu          sync.RWMutex
	connections chan *Connect
	closed      int32
	activeCount int32 // 当前活跃连接数

	ctx    context.Context
	cancel context.CancelFunc
}

func NewConnPool(
	target string,
	maxSize int,
	idleTimeout time.Duration,
	maxLifetime time.Duration,
	opts ...grpc.DialOption,
) (*ConnPool, error) {

	if maxSize <= 0 {
		return nil, errors.New("invalid maxSize")
	}

	ctx, cancel := context.WithCancel(context.Background())
	pool := &ConnPool{
		target:      target,
		maxSize:     maxSize,
		idleTimeout: idleTimeout,
		maxLifetime: maxLifetime,
		connections: make(chan *Connect, maxSize),
		dialOptions: opts,
		ctx:         ctx,
		cancel:      cancel,
	}

	// 启动后台健康检查
	go pool.healthCheckLoop()
	return pool, nil
}

func (p *ConnPool) createConnection() (*Connect, error) {
	atomic.AddInt32(&p.activeCount, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		p.target,
		append(p.dialOptions,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
		)...,
	)
	if err != nil {
		atomic.AddInt32(&p.activeCount, -1)
		return nil, err
	}

	return &Connect{
		ClientConn:  conn,
		createdAt:   time.Now(),
		lastUsed:    time.Now(),
		lastChecked: time.Now(),
	}, nil
}

func (p *ConnPool) Get(ctx context.Context) (*Connect, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return nil, errors.New("pool is closed")
	}

	select {
	case conn := <-p.connections:
		// 检查连接健康状态（根据 idleTimeout 决定是否需要验证）
		if conn.ShouldCheckHealth(p.idleTimeout) {
			if err := conn.Validate(1 * time.Second); err != nil {
				conn.Close()
				atomic.AddInt32(&p.activeCount, -1)
				return p.createConnection()
			}
		}

		// 检查连接生命周期
		if conn.IsExpired(p.maxLifetime) {
			conn.Close()
			atomic.AddInt32(&p.activeCount, -1)
			return p.createConnection()
		}

		conn.lastUsed = time.Now()
		return conn, nil

	case <-ctx.Done():
		// 用户取消或超时
		return nil, ctx.Err()

	default:
		// 无空闲连接时尝试创建新连接
		p.mu.Lock()
		defer p.mu.Unlock()

		if atomic.LoadInt32(&p.activeCount) < int32(p.maxSize) {
			conn, err := p.createConnection()
			if err != nil {
				return nil, err
			}
			return conn, nil
		}
		return nil, errors.New("connection pool exhausted")
	}
}

func (p *ConnPool) Put(conn *Connect) {
	if conn == nil || atomic.LoadInt32(&p.closed) == 1 {
		if conn != nil {
			conn.Close()
			atomic.AddInt32(&p.activeCount, -1)
		}
		return
	}

	// 仅当连接健康且未过期时归还
	if !conn.IsExpired(p.maxLifetime) && conn.ShouldCheckHealth(p.idleTimeout/2) {
		if err := conn.Validate(100 * time.Millisecond); err == nil {
			select {
			case p.connections <- conn:
				return
			default:
				// 池满时直接关闭
				conn.Close()
				atomic.AddInt32(&p.activeCount, -1)
				return
			}
		}
	}

	conn.Close()
	atomic.AddInt32(&p.activeCount, -1)
}

func (p *ConnPool) Close() {
	atomic.StoreInt32(&p.closed, 1)
	p.cancel() // 取消上下文，触发 healthCheckLoop 退出
	close(p.connections)
	for conn := range p.connections {
		conn.Close()
	}
}

func (p *ConnPool) healthCheckLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.mu.Lock()
			conns := make([]*Connect, 0, len(p.connections))
			for len(p.connections) > 0 {
				conn := <-p.connections
				conns = append(conns, conn)
			}

			for _, conn := range conns {
				if conn.IsExpired(p.maxLifetime) || conn.Validate(1*time.Second) != nil {
					conn.Close()
					atomic.AddInt32(&p.activeCount, -1)
				} else {
					p.connections <- conn
				}
			}
			p.mu.Unlock()

		case <-p.ctx.Done(): // 现在可以正常使用
			log.Println("健康检查协程退出")
			return
		}
	}
}

// ====================== 使用示例 ======================
//func main() {
//	// 创建上下文用于控制主流程
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	// 1. 创建连接池 (目标地址为K8s Service DNS)
//	pool, err := NewConnPool(
//		"dns:///my-service.namespace.svc.cluster.local:50051",
//		10,             // 最大连接数
//		2*time.Minute,  // 空闲超时时间
//		10*time.Minute, // 连接最大生命周期
//		grpc.WithKeepaliveParams(keepalive.ClientParameters{
//			Time:    30 * time.Second,
//			Timeout: 10 * time.Second,
//		}),
//	)
//	if err != nil {
//		log.Fatalf("创建连接池失败: %v", err)
//	}
//	defer pool.Close()
//
//	// 2. 模拟并发请求
//	var wg sync.WaitGroup
//	for i := 0; i < 20; i++ {
//		wg.Add(1)
//		go func(id int) {
//			defer wg.Done()
//
//			// 获取连接（带超时）
//			conn, err := pool.Get(context.Background())
//			if err != nil {
//				log.Printf("[%d] 获取连接失败: %v", id, err)
//				return
//			}
//			defer pool.Put(conn) // 使用后归还
//
//			// 执行RPC调用 (伪代码)
//			// client := pb.NewServiceClient(conn.ClientConn)
//			// resp, err := client.Method(context.Background(), &req)
//			log.Printf("[%d] 使用连接成功 | 状态: %s", id, conn.GetState())
//		}(i)
//	}
//	wg.Wait()
//	log.Println("所有请求处理完成")
//}
