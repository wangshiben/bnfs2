package messagequeue

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestIocContainer_AddData(t *testing.T) {
	ioc := &iocContainer{
		dataMap:     make(map[string]interface{}),
		lock:        sync.RWMutex{},
		channelMap:  make(map[string]chan interface{}),
		funcLock:    sync.RWMutex{},
		callFuncMap: make(map[string]callFunc),
	}

	// 测试添加数据
	key := "testKey"
	value := "testValue"
	ioc.addData(key, value)

	// 验证数据是否正确添加
	if ioc.getIOCData(key) != value {
		t.Errorf("Expected %v, got %v", value, ioc.getIOCData(key))
	}
}

func TestIocContainer_GetIOCData(t *testing.T) {
	ioc := &iocContainer{
		dataMap:     make(map[string]interface{}),
		lock:        sync.RWMutex{},
		channelMap:  make(map[string]chan interface{}),
		funcLock:    sync.RWMutex{},
		callFuncMap: make(map[string]callFunc),
	}

	// 测试获取不存在的数据
	key := "nonExistentKey"
	if ioc.getIOCData(key) != nil {
		t.Errorf("Expected nil, got %v", ioc.getIOCData(key))
	}

	// 添加数据后测试获取
	value := "testValue"
	ioc.dataMap[key] = value
	if ioc.getIOCData(key) != value {
		t.Errorf("Expected %v, got %v", value, ioc.getIOCData(key))
	}
}

func TestIocContainer_RegisterFunc(t *testing.T) {
	ioc := &iocContainer{
		dataMap:     make(map[string]interface{}),
		lock:        sync.RWMutex{},
		channelMap:  make(map[string]chan interface{}),
		funcLock:    sync.RWMutex{},
		callFuncMap: make(map[string]callFunc),
	}

	key := "testFunc"
	called := false
	var calledValue interface{}

	// 注册一个函数
	ioc.registerFunc(key, func(data interface{}) {
		called = true
		calledValue = data
	})

	// 验证函数是否注册成功
	if ioc.callFuncMap[key] == nil {
		t.Error("Function was not registered")
	}

	// 验证通道是否创建
	if ioc.channelMap[key] == nil {
		t.Error("Channel was not created")
	}

	// 测试函数调用
	testData := "testData"
	ioc.callFuncMap[key](testData)

	if !called {
		t.Error("Registered function was not called")
	}

	if calledValue != testData {
		t.Errorf("Expected %v, got %v", testData, calledValue)
	}
}

func TestIocContainer_StartListen(t *testing.T) {
	ioc := &iocContainer{
		dataMap:     make(map[string]interface{}),
		lock:        sync.RWMutex{},
		channelMap:  make(map[string]chan interface{}),
		funcLock:    sync.RWMutex{},
		callFuncMap: make(map[string]callFunc),
	}

	key := "testListener"
	resultChan := make(chan interface{}, 1)

	// 注册函数
	ioc.registerFunc(key, func(data interface{}) {
		resultChan <- data
		if data != "listenTestData" {
			t.Error("key error")
		}
	})

	// 启动监听（在goroutine中）
	go ioc.startListen(key)

	// 等待一点时间确保监听已启动
	time.Sleep(100 * time.Millisecond)

	// 发送数据
	testData := "listenTestData"
	ioc.addData(key, testData)

	// 等待结果
	select {
	case result := <-resultChan:
		if result != testData {
			t.Errorf("Expected %v, got %v", testData, result)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for function to be called")
	}
}

func TestIocContainer_AddDataWithFunc(t *testing.T) {
	ioc := &iocContainer{
		dataMap:     make(map[string]interface{}),
		lock:        sync.RWMutex{},
		channelMap:  make(map[string]chan interface{}),
		funcLock:    sync.RWMutex{},
		callFuncMap: make(map[string]callFunc),
	}

	key := "testAddWithFunc"
	resultChan := make(chan interface{}, 1)

	// 注册函数
	ioc.registerFunc(key, func(data interface{}) {
		resultChan <- data
	})

	// 启动监听（在goroutine中）
	go ioc.startListen(key)

	// 等待一点时间确保监听已启动
	time.Sleep(10 * time.Millisecond)

	// 添加数据，应该触发函数调用
	testData := "addDataWithFuncTest"
	ioc.addData(key, testData)

	// 等待结果
	select {
	case result := <-resultChan:
		if result != testData {
			t.Errorf("Expected %v, got %v", testData, result)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for function to be called")
	}

	// 验证数据也被存储
	if ioc.getIOCData(key) != testData {
		t.Errorf("Expected %v, got %v", testData, ioc.getIOCData(key))
	}
}

func TestIocContainer_JsonFormatOutput(t *testing.T) {
	ioc := &iocContainer{
		dataMap:     make(map[string]interface{}),
		lock:        sync.RWMutex{},
		channelMap:  make(map[string]chan interface{}),
		funcLock:    sync.RWMutex{},
		callFuncMap: make(map[string]callFunc),
	}

	// 设置测试数据
	currentIndexKey := "currentIndex"
	currentShellKey := "currentShell"
	currentIndexValue := 1
	currentShellValue := "echo 'Hello World'"

	ioc.addData(currentIndexKey, currentIndexValue)
	ioc.addData(currentShellKey, currentShellValue)

	// 模拟获取数据并格式化为JSON
	currentShellIndex := ioc.getIOCData(currentIndexKey)
	currentShell := ioc.getIOCData(currentShellKey)

	data := make(map[string]interface{})
	data["currentShell"] = currentShell
	data["currentShellIndex"] = currentShellIndex
	type BaseMassage struct {
		Type int         `json:"type"` //消息类型
		Data interface{} `json:"data"` //消息内容
	}
	// 创建消息结构体
	mess := &BaseMassage{
		Type: 1,
		Data: data,
	}

	marshal, err := json.Marshal(mess)
	if err != nil {
		t.Error(err)
	}

	t.Logf("json: %s", string(marshal))
}
