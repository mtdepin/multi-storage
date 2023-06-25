package config

type LoggerConfig struct {
	Level string
}

type JaegerConfig struct {
	Url string
}

type CommonNodeConfig struct {
	Id         string
	Name       string
	Node_group string
	Region     string
}

type MqConfig struct {
	Server             string
	Topic              string
	Topic_heartBeat    string
	Broadcast_group_id string
}

type RequestConfig struct {
	Max     int
	TimeOut int
}

type ProfileConfig struct {
	Enable bool
}

type RedisConfig struct {
	Url      string
	Password string
}
