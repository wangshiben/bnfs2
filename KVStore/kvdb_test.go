package KVStore

import (
	"os"
	"testing"
)

// 测试辅助函数：创建临时目录用于测试
func setupTestDB(t *testing.T) (*KvDBImp, string) {
	err := os.Mkdir("badger_test", os.ModeDir)
	// 修改：忽略目录已存在的错误，只要不是其他严重错误即可继续
	if err != nil && !os.IsExist(err) {
		t.Fatalf("创建临时目录失败：%v", err)
	}

	db, err := New("badger_test")
	if err != nil {
		os.RemoveAll("badger_test")
		t.Fatalf("创建 KvDB 失败：%v", err)
	}

	return db.(*KvDBImp), "badger_test"
}

// TestKvDB_GetAndDelete 测试读取和删除功能
func TestKvDB_GetAndDelete(t *testing.T) {
	db, _ := setupTestDB(t)
	//defer os.RemoveAll(dir) // 确保不删除目录，以便后续测试使用
	defer db.Close()

	key := "test_key"
	value := "test_value"
	persistKey := "persist_key" // 用于跨测试持久化的键
	persistValue := "persist_value"

	// 1. 先写入数据
	err := db.Put(key, value)
	if err != nil {
		t.Fatalf("Put 失败：%v", err)
	}

	// 2. 测试 Get 读取存在的数据
	gotValue, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get 存在的关键字失败：%v", err)
	}
	if gotValue != value {
		t.Errorf("Get 返回值不符，期望：%s, 得到：%s", value, gotValue)
	}

	// 3. 测试 Get 读取不存在的数据
	nonExistentKey := "no_such_key"
	_, err = db.Get(nonExistentKey)
	if err == nil {
		t.Error("期望获取不存在的键时返回错误，但得到 nil")
	}

	// 4. 测试 Delete 删除存在的键 (删除 test_key，但保留 persist_key)
	err = db.Delete(key)
	if err != nil {
		t.Fatalf("Delete 存在的键失败：%v", err)
	}

	// 5. 验证删除后 Get 应该返回错误
	_, err = db.Get(key)
	if err == nil {
		t.Error("期望删除后获取键返回错误，但得到 nil")
	}

	// 6. 【关键修改】写入一条需要持久化的数据，供下一个测试函数使用
	err = db.Put(persistKey, persistValue)
	if err != nil {
		t.Fatalf("写入持久化数据失败：%v", err)
	}
	t.Logf("已写入持久化数据: %s=%s，供后续测试加载", persistKey, persistValue)

	// 7. 测试 Delete 删除不存在的键
	err = db.Delete(nonExistentKey)
	if err != nil {
		t.Logf("删除不存在的键返回提示（非致命）：%v", err)
	}

	// 注意：此处不调用 os.RemoveAll，依靠 BadgerDB 的文件持久性
}

// TestKvDB_LoadExistingData 测试从现有数据库中加载数据
// 依赖前提：TestKvDB_GetAndDelete 已经运行并写入了 "persist_key"
func TestKvDB_LoadExistingData(t *testing.T) {
	// 直接使用相同的目录路径
	dbPath := "badger_test"

	// 检查目录是否存在，如果不存在则跳过或报错（取决于预期）
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Skipf("目录 %s 不存在，请先运行 TestKvDB_GetAndDelete", dbPath)
		return
	}

	// 打开现有的数据库实例
	db, err := New(dbPath)
	if err != nil {
		t.Fatalf("打开现有 KvDB 失败：%v", err)
	}
	defer db.Close()

	kvImp := db.(*KvDBImp)

	// 尝试加载在 TestKvDB_GetAndDelete 中写入的持久化数据
	targetKey := "persist_key"
	expectedValue := "persist_value"

	val, err := kvImp.Get(targetKey)
	if err != nil {
		t.Fatalf("从现有数据库加载键 %s 失败：%v", targetKey, err)
	}

	if val != expectedValue {
		t.Errorf("加载的数据值不符，期望：%s, 得到：%s", expectedValue, val)
	} else {
		t.Logf("成功从现有数据库加载数据：%s=%s", targetKey, val)
	}
}

// TestKvDB_CloseAndOperate 测试关闭后的操作
func TestKvDB_CloseAndOperate(t *testing.T) {
	db, dir := setupTestDB(t)
	defer os.RemoveAll(dir)

	// 写入一条数据
	_ = db.Put("before_close", "value")

	// 关闭数据库
	err := db.Close()
	if err != nil {
		t.Fatalf("Close 失败：%v", err)
	}

	// 尝试在关闭后读取
	_, err = db.Get("before_close")
	if err == nil {
		t.Error("期望在关闭后读取返回错误，但得到 nil")
	}

	// 尝试在关闭后删除
	err = db.Delete("before_close")
	if err == nil {
		t.Error("期望在关闭后删除返回错误，但得到 nil")
	}
}
