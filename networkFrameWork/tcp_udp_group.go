package networkFrameWork

import (
	"bnfs2/network"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

// TcpUdpGroup 实现 NetWorkGroup 接口，支持单端口同时监听 TCP 和 UDP
type TcpUdpGroup struct {
	addr        string
	handlers    map[string]Handler // 修改为地图以支持多个 Handler
	listeners   []net.Listener
	packetConns []net.PacketConn
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	connWg      sync.WaitGroup // 新增：用于跟踪正在处理的连接/任务
	mu          sync.RWMutex
}

// NewTcpUdpGroup 创建一个新的同时监听 TCP 和 UDP 的组
func NewTcpUdpGroup(addr string) *TcpUdpGroup {
	ctx, cancel := context.WithCancel(context.Background())
	return &TcpUdpGroup{
		addr:   addr,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Addr 返回监听地址
func (g *TcpUdpGroup) Addr() string {
	return g.addr
}

// ReceiveStream 此方法在并发监听模型下不适用，因为连接是异步到达的。
// 该接口定义可能更偏向于拉取模式，但在监听服务器模式下，我们通过注册 Handler 来推送处理。
// 这里返回 nil 或抛出错误，提示用户应使用 RegisterHandler。
func (g *TcpUdpGroup) ReceiveStream() network.Stream {
	return nil
}

// GetHandler 根据路由名称获取处理器
func (g *TcpUdpGroup) GetHandler(routeName string) Handler {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.handlers == nil {
		return nil
	}
	return g.handlers[routeName]
}

// RegisterHandler 注册处理函数，并启动监听
func (g *TcpUdpGroup) RegisterHandler(handlerName string, handler Handler) {
	g.mu.Lock()
	if g.handlers == nil {
		g.handlers = make(map[string]Handler)
	}
	g.handlers[handlerName] = handler
	g.mu.Unlock()

	// 仅在第一次注册时启动监听（简单判断，实际可根据状态位控制）
	// 这里为了简单，每次注册都尝试启动，startListening 内部可做幂等处理或外部保证只调一次
	// 修正：只在没有监听器时启动
	g.mu.RLock()
	hasListeners := len(g.listeners) > 0 || len(g.packetConns) > 0
	g.mu.RUnlock()

	if !hasListeners {
		go g.startListening()
	}
}

// startListening 解析地址并启动 TCP 和 UDP 监听
func (g *TcpUdpGroup) startListening() {
	host, port, err := net.SplitHostPort(g.addr)
	if err != nil {
		fmt.Printf("地址解析失败：%v\n", err)
		return
	}

	// 启动 TCP 监听
	tcpAddr := fmt.Sprintf("%s:%s", host, port)
	tcpListener, err := net.Listen("tcp", tcpAddr)
	log.Println("开始监听TCP")
	if err != nil {
		fmt.Printf("TCP 监听失败 (%s): %v\n", tcpAddr, err)
	} else {
		g.listeners = append(g.listeners, tcpListener)
		g.wg.Add(1)
		go g.acceptTcpLoop(tcpListener)
	}

	// 启动 UDP 监听
	udpAddr := fmt.Sprintf("%s:%s", host, port)
	udpConn, err := net.ListenPacket("udp", udpAddr)
	if err != nil {
		fmt.Printf("UDP 监听失败 (%s): %v\n", udpAddr, err)
	} else {
		g.packetConns = append(g.packetConns, udpConn)
		g.wg.Add(1)
		go g.readUdpLoop(udpConn.(*net.UDPConn))
		log.Println("开始监听")
	}
}

// acceptTcpLoop 处理 TCP 连接
func (g *TcpUdpGroup) acceptTcpLoop(listener net.Listener) {
	defer g.wg.Done()
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-g.ctx.Done():
				return
			default:
				continue
			}
		}

		// 预读取 256 字节头部
		headerBuf := make([]byte, 256)
		_, err = io.ReadFull(conn, headerBuf)
		if err != nil {
			conn.Close()
			// 记录日志：读取头部失败
			continue
		}
		header, err := ParseHeader(headerBuf)
		if err != nil {
			return
		}
		// 提取前 20 字节作为 HandlerName (去除可能的空字符)
		routeName := header.RouteName

		// 获取对应的 Handler
		g.mu.RLock()
		handler := g.handlers[routeName]
		g.mu.RUnlock()

		if handler == nil {
			// 未找到处理器，关闭连接
			conn.Close()
			continue
		}
		PayLoad := make([]byte, header.PayLoadLength)
		_, err = io.ReadFull(conn, PayLoad)
		if err != nil {
			conn.Close()
			// 记录日志：读取头部失败
			continue
		}
		// 创建带缓冲的流，将已读取的 256 字节放回流中，以便 Handler 能读到完整数据
		stream := &bufferedStream{
			Stream: &tcpStream{Conn: conn},
			buffer: append(headerBuf, PayLoad...),
		}

		g.connWg.Add(1)
		go func() {
			defer g.connWg.Done()
			if err := handler(stream); err != nil {
				// 可选：记录错误日志
			}
			// 处理完成后，关闭连接以发送 FIN，避免客户端收到 RST
			conn.Close()
		}()
	}
}

// readUdpLoop 处理 UDP 数据包
func (g *TcpUdpGroup) readUdpLoop(conn *net.UDPConn) {
	defer g.wg.Done()

	streamMap := make(map[string]*bufferedUdpStream)
	var mapMu sync.Mutex

	buffer := make([]byte, 65535)

	for {
		n, remoteAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			select {
			case <-g.ctx.Done():
				return
			default:
				continue
			}
		}

		addrKey := remoteAddr.String()

		mapMu.Lock()
		stream, exists := streamMap[addrKey]

		if !exists {
			// 新的客户端，必须确保接收到的数据包含完整的 256 字节头
			if n < 256 {
				mapMu.Unlock()
				continue
			}

			headerBuf := make([]byte, 256)
			copy(headerBuf, buffer[:256])
			header, err := ParseHeader(headerBuf)
			if err != nil {
				return
			}
			routeName := header.RouteName

			g.mu.RLock()
			handler := g.handlers[routeName]
			g.mu.RUnlock()

			if handler == nil {
				mapMu.Unlock()
				continue
			}

			remainingData := buffer[256:n]

			udpStreamBase := &udpStream{
				conn:       conn,
				remoteAddr: remoteAddr,
				inChan:     make(chan []byte, 100),
				closeChan:  make(chan struct{}),
			}

			if len(remainingData) > 0 {
				dataCopy := make([]byte, len(remainingData))
				copy(dataCopy, remainingData)
				udpStreamBase.inChan <- dataCopy
			}

			stream = &bufferedUdpStream{
				Stream: udpStreamBase,
				buffer: headerBuf,
			}
			streamMap[addrKey] = stream

			g.connWg.Add(1)
			go func(s *bufferedUdpStream, key string) {
				defer g.connWg.Done()
				if err := handler(s); err != nil {
					mapMu.Lock()
					delete(streamMap, key)
					mapMu.Unlock()
					s.Close()
					return
				}
				// UDP 虚拟流在处理完后，可以选择从 map 中移除并关闭
				// 这里依赖 handler 内部逻辑或超时，暂时保持原有逻辑，仅在 error 时删除
				// 为了资源安全，可以在 handler 正常返回后也尝试清理，但需小心并发
				// 简单起见，维持原样，依靠测试结束时的全局关闭
			}(stream, addrKey)

			mapMu.Unlock()
		} else {
			// 已存在的流
			data := make([]byte, n)
			copy(data, buffer[:n])

			mapMu.Unlock()

			select {
			case stream.Stream.(*udpStream).inChan <- data:
			case <-stream.Stream.(*udpStream).closeChan:
			}
			continue
		}
	}
}

// Close 关闭所有监听和连接
func (g *TcpUdpGroup) Close() error {
	g.cancel()

	var errs []error
	for _, l := range g.listeners {
		if err := l.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, p := range g.packetConns {
		if err := p.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// 等待主监听循环退出
	g.wg.Wait()

	// 新增：等待所有正在处理的连接/任务完成
	g.connWg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}

// tcpStream 简单的包装器，确保符合 network.Stream 接口
type tcpStream struct {
	net.Conn
}

func (t *tcpStream) SendMessage(ctx context.Context, message []byte) ([]byte, error) {
	// 发送消息
	_, err := t.Write(message)
	if err != nil {
		return nil, err
	}

	// 接收响应 (假设协议是请求 - 响应模式，读取直到错误或特定长度，这里简单读取一次)
	// 注意：实际生产中需要更复杂的协议解析（如长度前缀）
	buf := make([]byte, 4096)
	n, err := t.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// udpStream 模拟 UDP 的流式接口
type udpStream struct {
	conn       *net.UDPConn
	remoteAddr *net.UDPAddr
	inChan     chan []byte
	closeChan  chan struct{}
	closed     bool
	mu         sync.Mutex
}

func (u *udpStream) Read(p []byte) (n int, err error) {
	select {
	case data := <-u.inChan:
		n = copy(p, data)
		return n, nil
	case <-u.closeChan:
		return 0, errors.New("stream closed")
	}
}

func (u *udpStream) Write(p []byte) (n int, err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return 0, errors.New("stream closed")
	}
	return u.conn.WriteToUDP(p, u.remoteAddr)
}

func (u *udpStream) Close() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if !u.closed {
		u.closed = true
		close(u.closeChan)
	}
	return nil
}

func (u *udpStream) SendMessage(ctx context.Context, message []byte) ([]byte, error) {
	// 发送
	_, err := u.Write(message)
	if err != nil {
		return nil, err
	}

	// 接收响应
	// 使用 select 支持上下文取消
	select {
	case data := <-u.inChan:
		return data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-u.closeChan:
		return nil, errors.New("stream closed")
	}
}

// bufferedStream 用于包装已经预读了头部的流
type bufferedStream struct {
	network.Stream
	buffer []byte
	offset int
}

func (b *bufferedStream) Read(p []byte) (n int, err error) {
	if b.offset < len(b.buffer) {
		n = copy(p, b.buffer[b.offset:])
		b.offset += n
		return n, nil
	}
	// 缓冲区读完，直接读取底层流
	return b.Stream.Read(p)
}

// 确保其他方法透传
func (b *bufferedStream) SendMessage(ctx context.Context, message []byte) ([]byte, error) {
	return b.Stream.SendMessage(ctx, message)
}

// bufferedUdpStream 用于 UDP 的缓冲包装
type bufferedUdpStream struct {
	network.Stream
	buffer []byte
	offset int
}

func (b *bufferedUdpStream) Read(p []byte) (n int, err error) {
	if b.offset < len(b.buffer) {
		n = copy(p, b.buffer[b.offset:])
		b.offset += n
		return n, nil
	}
	return b.Stream.Read(p)
}

func (b *bufferedUdpStream) SendMessage(ctx context.Context, message []byte) ([]byte, error) {
	return b.Stream.SendMessage(ctx, message)
}
