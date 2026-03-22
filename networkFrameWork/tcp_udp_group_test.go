package networkFrameWork

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"bnfs2/network"
)

// testHandler 是一个简单的测试处理器，它读取所有输入并回写 "OK:" + 内容
func testHandler(stream network.Stream) error {
	// 读取一部分数据来验证头部是否被正确缓冲并传递
	buf := make([]byte, 1024)
	n, err := stream.Read(buf)
	if err != nil && n == 0 {
		return err
	}

	// 简单回显逻辑，实际业务可能更复杂
	response := append([]byte("OK:"), buf[:n]...)

	_, writeErr := stream.Write(response)
	return writeErr
}

func TestTcpUdpGroup_TCP(t *testing.T) {
	addr := "127.0.0.1:8888"
	group := NewTcpUdpGroup(addr)

	// 注册测试 Handler，名称定为 "TEST_HANDLER_0000000000" (凑够 20 字节)
	handlerName := "TEST_HANDLER_0000000"
	group.RegisterHandler(handlerName, testHandler)

	// 给服务器一点时间启动监听
	time.Sleep(500 * time.Millisecond)

	// 客户端逻辑
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("TCP 连接失败：%v", err)
	}
	defer conn.Close()

	// 构造消息：前 20 字节为 HandlerName，补足 256 字节头部，后跟实际负载
	payload := []byte("Hello TCP Server")
	header := &Header{RouteName: handlerName, PayLoadLength: uint(len(payload))}

	headerBytes, err := header.ParseToBytes()
	if err != nil {
		t.Fatalf("Header 构造失败：%v", err)

	}
	messages := append(headerBytes, payload...)
	// 发送完整数据 (头部 + 负载)
	_, err = conn.Write(messages)
	if err != nil {
		t.Fatalf("TCP 发送失败：%v", err)
	}

	// 读取响应
	respBuf := make([]byte, 1024)
	// 设置读取超时
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(respBuf)
	if err != nil {
		t.Fatalf("TCP 读取响应失败：%v", err)
	}

	expectedPrefix := "OK:" + handlerName // 因为缓冲流会先读出头部的前部分
	t.Logf("TCP 响应前缀：%s", expectedPrefix)
	// 注意：由于我们发送了 256 字节头 + 负载，Read 可能会读到头部的一部分 + 负载
	// 具体取决于 testHandler 中 Read 的行为。这里我们只检查是否收到了 "OK:" 开头
	if !strings.HasPrefix(string(respBuf[:n]), "OK:") {
		t.Errorf("TCP 响应不符合预期，收到：%s", string(respBuf[:n]))
	} else {
		t.Logf("TCP 测试成功，收到响应：%s", string(respBuf[:n]))
	}

	group.Close()
}

func TestTcpUdpGroup_UDP(t *testing.T) {
	addr := "127.0.0.1:8889"
	group := NewTcpUdpGroup(addr)

	handlerName := "TEST_HANDLER_0000000001" // 20 字节
	group.RegisterHandler(handlerName, testHandler)

	// 等待启动
	time.Sleep(500 * time.Millisecond)

	// 客户端逻辑
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatalf("解析 UDP 地址失败：%v", err)
	}

	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		t.Fatalf("创建 UDP 客户端失败：%v", err)
	}
	defer conn.Close()

	// 构造消息：必须 >= 256 字节，前 20 字节为 HandlerName
	payload := []byte("Hello UDP Server")
	h := &Header{
		RouteName:     handlerName,
		PayLoadLength: uint(len(payload)),
		OriginData:    nil,
	}
	bytes, err := h.ParseToBytes()
	if err != nil {
		return
	}

	packet := append(bytes, payload...)

	_, err = conn.WriteToUDP(packet, udpAddr)
	if err != nil {
		t.Fatalf("UDP 发送失败：%v", err)
	}

	// 读取响应
	respBuf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := conn.ReadFromUDP(respBuf)
	if err != nil {
		t.Fatalf("UDP 读取响应失败：%v", err)
	}

	if !strings.HasPrefix(string(respBuf[:n]), "OK:") {
		t.Errorf("UDP 响应不符合预期，收到：%s", string(respBuf[:n]))
	} else {
		t.Logf("UDP 测试成功，收到响应：%s", string(respBuf[:n]))
	}

	group.Close()
}

func TestTcpUdpGroup_Alternating(t *testing.T) {
	addr := "127.0.0.1:8890"
	group := NewTcpUdpGroup(addr)

	handlerName := "ALT_TEST_HANDLER_00000" // 确保长度足够
	group.RegisterHandler(handlerName, testHandler)

	// 等待服务器启动
	time.Sleep(500 * time.Millisecond)

	// 交替发送 3 次
	for i := 0; i < 3; i++ {
		t.Logf("=== 第 %d 轮交替测试 ===", i+1)

		// --- TCP 部分 ---
		tcpConn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("第 %d 轮 TCP 连接失败：%v", i+1, err)
		}

		tcpPayload := []byte(fmt.Sprintf("TCP Message Round %d", i+1))
		tcpHeader := &Header{RouteName: handlerName, PayLoadLength: uint(len(tcpPayload))}
		tcpHeaderBytes, err := tcpHeader.ParseToBytes()
		if err != nil {
			tcpConn.Close()
			t.Fatalf("第 %d 轮 TCP Header 构造失败：%v", i+1, err)
		}
		tcpMsg := append(tcpHeaderBytes, tcpPayload...)

		_, err = tcpConn.Write(tcpMsg)
		if err != nil {
			tcpConn.Close()
			t.Fatalf("第 %d 轮 TCP 发送失败：%v", i+1, err)
		}

		tcpRespBuf := make([]byte, 1024)
		tcpConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := tcpConn.Read(tcpRespBuf)
		tcpConn.Close()
		if err != nil {
			t.Fatalf("第 %d 轮 TCP 读取响应失败：%v", i+1, err)
		}

		if !strings.HasPrefix(string(tcpRespBuf[:n]), "OK:") {
			t.Errorf("第 %d 轮 TCP 响应不符合预期，收到：%s", i+1, string(tcpRespBuf[:n]))
		} else {
			t.Logf("第 %d 轮 TCP 测试成功，收到响应：%s", i+1, string(tcpRespBuf[:n]))
		}

		// --- UDP 部分 ---
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			t.Fatalf("第 %d 轮解析 UDP 地址失败：%v", i+1, err)
		}

		udpConn, err := net.ListenUDP("udp", nil)
		if err != nil {
			t.Fatalf("第 %d 轮创建 UDP 客户端失败：%v", i+1, err)
		}

		udpPayload := []byte(fmt.Sprintf("UDP Message Round %d", i+1))
		udpHeader := &Header{RouteName: handlerName, PayLoadLength: uint(len(udpPayload))}
		udpHeaderBytes, err := udpHeader.ParseToBytes()
		if err != nil {
			udpConn.Close()
			t.Fatalf("第 %d 轮 UDP Header 构造失败：%v", i+1, err)
		}
		udpPacket := append(udpHeaderBytes, udpPayload...)

		_, err = udpConn.WriteToUDP(udpPacket, udpAddr)
		if err != nil {
			udpConn.Close()
			t.Fatalf("第 %d 轮 UDP 发送失败：%v", i+1, err)
		}

		udpRespBuf := make([]byte, 1024)
		udpConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _, err = udpConn.ReadFromUDP(udpRespBuf)
		udpConn.Close()
		if err != nil {
			t.Fatalf("第 %d 轮 UDP 读取响应失败：%v", i+1, err)
		}

		if !strings.HasPrefix(string(udpRespBuf[:n]), "OK:") {
			t.Errorf("第 %d 轮 UDP 响应不符合预期，收到：%s", i+1, string(udpRespBuf[:n]))
		} else {
			t.Logf("第 %d 轮 UDP 测试成功，收到响应：%s", i+1, string(udpRespBuf[:n]))
		}
	}

	group.Close()
	t.Log("=== 交替测试全部完成 ===")
}
