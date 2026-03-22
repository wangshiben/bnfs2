package network

import (
	"encoding/binary"
	"errors"
)

type Header struct {
	RouteName     string
	PayLoadLength uint
	OriginData    []byte
}
type Message struct {
	Header  *Header
	Payload []byte
}

const MagicHeader = "bnfs-data"
const HeaderLength = 256

var MagicHeaderBytes = []byte(MagicHeader)

func ParseHeader(headerBytes []byte) (*Header, error) {
	index := 0

	// 1. 验证魔数
	for ; index < len(MagicHeaderBytes); index++ {
		if headerBytes[index] != MagicHeaderBytes[index] {
			return nil, errors.New("invalid header")
		}
	}

	// 2. 读取路由名长度 (8 bytes, LittleEndian)
	RouteNameLength := make([]byte, 8)
	currentIndex := index
	for ; index < (8 + currentIndex); index++ {
		RouteNameLength[index-currentIndex] = headerBytes[index]
	}
	nameLength := parseBytesToInt64(RouteNameLength)

	// 3. 读取路由名
	if int(nameLength) < 0 || int(nameLength) > (HeaderLength-len(MagicHeaderBytes)-8-8) {
		return nil, errors.New("invalid header")
	}
	RouteName := string(headerBytes[index : index+int(nameLength)])
	index += int(nameLength)

	// 4. 读取负载长度 (8 bytes, LittleEndian) - 新增字段
	PayLoadLengthBytes := make([]byte, 8)
	currentIndex = index
	for ; index < (8 + currentIndex); index++ {
		PayLoadLengthBytes[index-currentIndex] = headerBytes[index]
	}
	payloadLength := parseBytesToInt64(PayLoadLengthBytes)

	return &Header{
		RouteName:     RouteName,
		PayLoadLength: payloadLength,
		OriginData:    headerBytes[:],
	}, nil
}

func parseBytesToInt64(data []byte) uint {
	res := binary.LittleEndian.Uint64(data)
	return uint(res)
}

// ParseToBytes 将路由名和负载长度序列化为符合协议定义的 256 字节头部
// 结构：[MagicHeader(9 bytes)] + [RouteNameLength(8 bytes, LittleEndian)] + [RouteName] + [PayloadLength(8 bytes, LittleEndian)] + [Padding zeros]
func (h *Header) ParseToBytes() ([]byte, error) {
	routeName := h.RouteName
	payloadLength := h.PayLoadLength
	headerBytes := make([]byte, HeaderLength)

	// 1. 写入魔数
	if len(MagicHeaderBytes) > HeaderLength {
		return nil, errors.New("magic header too large")
	}
	copy(headerBytes, MagicHeaderBytes)

	currentIndex := len(MagicHeaderBytes)

	// 2. 检查路由名长度是否合法
	// 最大可用长度 = 总长度 - 魔数长度 - 长度字段长度 (8 bytes) - 负载长度字段长度 (8 bytes)
	maxNameLength := HeaderLength - currentIndex - 8 - 8
	if len(routeName) > maxNameLength {
		return nil, errors.New("route name too long")
	}

	// 3. 写入路由名长度 (8 bytes, LittleEndian)
	nameLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(nameLenBytes, uint64(len(routeName)))
	copy(headerBytes[currentIndex:], nameLenBytes)
	currentIndex += 8

	// 4. 写入路由名
	copy(headerBytes[currentIndex:], routeName)
	currentIndex += len(routeName)

	// 5. 写入负载长度 (8 bytes, LittleEndian) - 新增字段
	payloadLenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(payloadLenBytes, uint64(payloadLength))
	copy(headerBytes[currentIndex:], payloadLenBytes)
	// 剩余部分默认为 0，无需额外操作，因为 make 初始化即为 0

	return headerBytes, nil
}

func (m *Message) ParseToBytes() ([]byte, error) {
	m.Header.PayLoadLength = uint(len(m.Payload))
	headerBytes, err := m.Header.ParseToBytes()
	if err != nil {
		return nil, err
	}
	return append(headerBytes, m.Payload...), nil
}
func ParseMessage(messageBytes []byte) (*Message, error) {
	header, err := ParseHeader(messageBytes[:HeaderLength])
	if err != nil {
		return nil, err
	}
	return &Message{
		Header:  header,
		Payload: messageBytes[HeaderLength:],
	}, nil
}
