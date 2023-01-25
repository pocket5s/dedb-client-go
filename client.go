package main

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"

	api "api/dedb"
)

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

type ClientConfig struct {
	RedisDbConfig RedisDbConfig
	Server        string
	Streams       []string
	ConsumerGroup string
	EventChannel  chan<- *api.Event
	ErrorChannel  chan<- error
}

type Client struct {
	config       ClientConfig
	server       *api.DeDBServer
	pool         *redis.Client
	eventChannel chan<- *api.Event
	errorChannel chan<- error
}

func NewClient(config ClientConfig) (*Client, error) {
	if config.Server == "" {
		return nil, fmt.Errorf("Server config entry required")
	}
	if len(config.Streams) == 0 {
		return nil, fmt.Errorf("Streams config entry required")
	}
	if config.ConsumerGroup == "" {
		return nil, fmt.Errorf("ConsumerGroup config entry required")
	}

	c := &Client{config: config}
	c.eventChannel = config.EventChannel
	c.errorChannel = config.ErrorChannel
	return c, nil
}

func (c *Client) Save(ctx context.Context, request *api.SaveRequest) (*api.SaveResponse, error) {
	return c.server.Save(ctx, request)
}

func (c *Client) GetDomain(ctx context.Context, request *api.GetDomainRequest) (*api.GetResponse, error) {
	return c.server.GetDomain(ctx, request)
}

func (c *Client) GetDomainIds(ctx context.Context, request *api.GetDomainIdsRequest) (*api.GetDomainIdsResponse, error) {
	return c.server.GetDomainIds(ctx, request)
}

func (c *Client) Connect(ctx context.Context) error {
	// connect to DeDB server

	// connect to redis server
	c.pool = internal.newPool(false, c.config, nil)
	return nil
}

func (c *Client) Close() {
}
