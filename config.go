package dedb_client_go

import "github.com/pocket5s/dedb-client-go/api"

type RedisDbConfig struct {
	Server        string `envconfig:"DEDB_REDIS_DB"`
	Password      string `envconfig:"DEDB_REDIS_DB_PASSWORD"`
	RedisCa       string `envconfig:"DEDB_REDIS_DB_CA"`
	RedisUserCert string `envconfig:"DEDB_REDIS_DB_USER_CERT"`
	RedisUserKey  string `envconfig:"DEDB_REDIS_DB_USER_KEY"`
	Index         string `envconfig:"DEDB_REDIS_DB_INDEX"`
	MinIdle       int    `envconfig:"DEDB_REDIS_DB_MINIDLE"`
	MaxActive     int    `envconfig:"DEDB_REDIS_DB_MAXACTIVE"`
	IdleTimeout   int64  `envconfig:"DEDB_REDIS_DB_IDLE_TIMEOUT"`
}

type RedisSearchConfig struct {
	Server        string `envconfig:"REDIS_SEARCH"`
	Password      string `envconfig:"REDIS_SEARCH_PASSWORD"`
	RedisCa       string `envconfig:"REDIS_SEARCH_CA"`
	RedisUserCert string `envconfig:"REDIS_SEARCH_USER_CERT"`
	RedisUserKey  string `envconfig:"REDIS_SEARCH_USER_KEY"`
	Index         string `envconfig:"REDIS_SEARCH_INDEX"`
	MinIdle       int    `envconfig:"REDIS_SEARCH_MINIDLE"`
	MaxActive     int    `envconfig:"REDIS_SEARCH_MAXACTIVE"`
	IdleTimeout   int64  `envconfig:"REDIS_SEARCH_IDLE_TIMEOUT"`
}

type ClientConfig struct {
	RedisDbConfig     RedisDbConfig
	RedisSearchConfig RedisSearchConfig
	Server            string
	Streams           []string
	ConsumerGroup     string
	EventChannel      chan<- *api.Event
	ErrorChannel      chan<- error
}
