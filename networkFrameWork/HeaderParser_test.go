package networkFrameWork

import (
	"bnfs2/network"
	"bytes"
	"testing"
)

// TestParseHeaderAndToBytes 测试 ParseToBytes 和 ParseHeader 的往返序列化
// 主要验证 RouteName 和 PayLoadLength 字段是否能正确序列化和反序列化
func TestParseHeaderAndToBytes(t *testing.T) {
	testCases := []struct {
		name          string
		routeName     string
		payloadLength uint
		expectError   bool
	}{
		{
			name:          "NormalCase",
			routeName:     "test_route_name",
			payloadLength: 1024,
			expectError:   false,
		},
		{
			name:          "EmptyRouteName",
			routeName:     "",
			payloadLength: 0,
			expectError:   false,
		},
		{
			name:          "MaxPayloadLength",
			routeName:     "short",
			payloadLength: 999999,
			expectError:   false,
		},
		{
			name:          "LongRouteName",
			routeName:     "this_is_a_very_long_route_name_that_is_still_within_limits",
			payloadLength: 500,
			expectError:   false,
		},
		{
			name:          "LongRouteName",
			routeName:     "this_is_a_very_long_route_name_that_is_still_within_limitsthis_is_a_very_long_route_name_that_is_still_within_limitsthis_is_a_very_long_route_name_that_is_still_within_limitsthis_is_a_very_long_route_name_that_is_still_within_limits",
			payloadLength: 500,
			expectError:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 1. 调用 ParseToBytes 生成字节流
			h := &network.Header{RouteName: tc.routeName, PayLoadLength: tc.payloadLength}
			headerBytes, err := h.ParseToBytes()
			if tc.expectError {
				if err == nil {
					t.Errorf("预期产生错误，但未收到错误")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseToBytes 失败：%v", err)
			}

			// 验证生成的头部长度是否为 256
			if len(headerBytes) != network.HeaderLength {
				t.Errorf("生成的头部长度应为 %d，实际为 %d", network.HeaderLength, len(headerBytes))
			}

			// 2. 调用 ParseHeader 解析字节流
			parsedHeader, err := network.ParseHeader(headerBytes)
			if err != nil {
				t.Fatalf("ParseHeader 失败：%v", err)
			}

			// 3. 断言 RouteName 一致
			if parsedHeader.RouteName != tc.routeName {
				t.Errorf("RouteName 不匹配：预期 %q，实际 %q", tc.routeName, parsedHeader.RouteName)
			}

			// 4. 断言 PayLoadLength 一致
			if parsedHeader.PayLoadLength != tc.payloadLength {
				t.Errorf("PayLoadLength 不匹配：预期 %d，实际 %d", tc.payloadLength, parsedHeader.PayLoadLength)
			}

			// 5. 验证魔数部分是否正确
			if !bytes.HasPrefix(headerBytes, network.MagicHeaderBytes) {
				t.Error("生成的头部魔数不正确")
			}

			t.Logf("测试通过：RouteName=%q, PayLoadLength=%d", parsedHeader.RouteName, parsedHeader.PayLoadLength)
		})
	}
}

// TestParseHeaderInvalidData 测试 ParseHeader 对非法数据的处理
func TestParseHeaderInvalidData(t *testing.T) {
	// 测试魔数错误的情况
	invalidHeader := make([]byte, network.HeaderLength)
	copy(invalidHeader, "wrong-magic")

	_, err := network.ParseHeader(invalidHeader)
	if err == nil {
		t.Error("预期解析非法魔数时返回错误，但未收到错误")
	}

	// 测试数据长度不足的情况
	shortHeader := make([]byte, 10)
	_, err = network.ParseHeader(shortHeader)
	if err == nil {
		t.Error("预期解析过短头部时返回错误，但未收到错误")
	}
}
