package messagequeue

type MqMessage struct {
	BasePath string      `json:"basePath"` // 执行路径
	Shell    string      `json:"shell"`    // 命令行
	Time     int64       `json:"time"`     // 执行时间
	Code     int         `json:"code"`
	Other    interface{} `json:"other"`
}
