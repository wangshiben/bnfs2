package messagequeue

type MqMessage struct {
	Data   []byte
	PubKey string
	Sign   []byte // 对Data的签名
	flag   int    //
}
