package StoreNode

import (
	"bnfs2/DHTable"
	"bnfs2/KVStore"
	"bnfs2/interfaces"
	"bnfs2/network"
	"bnfs2/networkFrameWork"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// NewUDPTCPGroupAndNode 创建并配置好处理器的网络组和节点
func NewUDPTCPGroupAndNode() (networkFrameWork.NetWorkGroup, interfaces.Node) {
	group := networkFrameWork.NewTcpUdpGroup(":8080")
	node, err := DHTable.NewNode()
	if err != nil {
		return nil, nil
	}
	// 注册 store 路由的处理函数
	group.RegisterHandler("store", StoreSlice(node))
	return group, node
}

func TestStoreSlice_TCP_UDP(t *testing.T) {
	// 1. 初始化网络和节点
	group, node := NewUDPTCPGroupAndNode()
	if group == nil || node == nil {
		t.Fatal("Failed to initialize network group or node")
	}
	defer group.Close() // 测试结束后停止服务

	// 等待服务启动
	time.Sleep(500 * time.Millisecond)

	// 2. 准备测试数据
	testData := []byte("this is a test slice content for bnfs storage verification.")

	// 计算期望的文件名 (SHA256 Hex)
	hasher := sha256.New()
	hasher.Write(testData)
	expectedFileName := hex.EncodeToString(hasher.Sum(nil))

	// 构造 Message
	header := &network.Header{
		RouteName: "store",
	}
	message := &network.Message{
		Header:  header,
		Payload: testData,
	}

	msgBytes, err := message.ParseToBytes()
	if err != nil {
		t.Fatalf("Failed to parse message to bytes: %v", err)
	}

	// 3. TCP 发送测试
	func() {
		conn, err := net.Dial("tcp", "127.0.0.1:8080")
		if err != nil {
			t.Fatalf("TCP Dial failed: %v", err)
		}
		defer conn.Close()

		_, err = conn.Write(msgBytes)
		if err != nil {
			t.Fatalf("TCP Write failed: %v", err)
		}
	}()
	time.Sleep(1 * time.Second)
	// 4. UDP 发送测试
	func() {
		addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:8080")
		if err != nil {
			t.Fatalf("ResolveUDPAddr failed: %v", err)
		}
		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			t.Fatalf("UDP Dial failed: %v", err)
		}
		defer conn.Close()

		_, err = conn.Write(msgBytes)
		if err != nil {
			t.Fatalf("UDP Write failed: %v", err)
		}
	}()

	// 5. 等待处理完成 (异步写入可能需要一点时间)
	time.Sleep(1 * time.Second)
	storageDir, err := storeDb.Get(KVStore.StoreLocation)
	if err != nil {
		t.Fatalf("StoreDB get error %v", err)
	}
	// 6. 验证文件是否存在
	filePath := filepath.Join(storageDir, expectedFileName)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Errorf("Expected file to exist at %s, but it does not. (Checked after TCP and UDP send)", filePath)
	} else if err != nil {
		t.Errorf("Error checking file status: %v", err)
	} else {
		// 可选：验证文件内容
		content, err := os.ReadFile(filePath)
		if err != nil {
			t.Errorf("Failed to read saved file: %v", err)
		} else if string(content) != string(testData) {
			t.Errorf("File content mismatch. Expected: %s, Got: %s", string(testData), string(content))
		}
	}
}
