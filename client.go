package dedb_client_go

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-redis/redis/v8"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/pocket5s/dedb-client-go/api"
)

type Client struct {
	config       ClientConfig
	server       api.DeDBClient
	pool         *redis.Client
	log          zerolog.Logger
	shutdown     bool
	streams      []string
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

	c := &Client{config: config}
	c.streams = make([]string, 0)
	c.log = log.With().Str("logger", "DeDBClient").Logger()
	for _, s := range config.Streams {
		c.streams = append(c.streams, "dedb:stream:"+s)
	}
	if config.ConsumerGroup == "" {
		return nil, fmt.Errorf("ConsumerGroup config entry required")
	}

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
	/*
		for c.server == nil {
			conn := c.connectToGrpcService(c.config.Server, "dedb")
			if conn != nil {
				c.server = api.NewDeDBClient(conn)
				c.log.Info().Msg("connected to DeDB")
			} else {
				time.Sleep(2 * time.Second)
			}
		}
	*/

	// connect to redis server
	pool, err := newPool(false, c.config, &c.log)
	if err != nil {
		return err
	}
	c.pool = pool
	c.log.Info().Msg("connected to redis")
	go c.listenForEvents()
	return nil
}

func (c *Client) Close() {
	c.shutdown = true
	c.pool.Close()
}

func (c *Client) listenForEvents() {
	// see if there is a consumer group already established for each stream
	c.log.Info().Msgf("checking for consumer group %s on each stream requested", c.config.ConsumerGroup)
	id := c.getConsumerId()
	streamArgs := make([]string, 0)
	for _, stream := range c.streams {
		groups, err := c.pool.XInfoGroups(context.Background(), stream).Result()
		if err != nil {
			c.log.Error().Err(err).Msgf("could not query for consumer groups on stream %s", stream)
		} else {
			var found bool
			for _, g := range groups {
				if g.Name == c.config.ConsumerGroup {
					found = true
				}
			}

			if !found {
				c.log.Info().Msgf("consumer group %s not found, creating it", c.config.ConsumerGroup)
				status, err := c.pool.XGroupCreate(context.Background(), stream, c.config.ConsumerGroup, "$").Result()
				if err != nil {
					c.log.Error().Err(err).Msgf("could not create group %s for stream %s", c.config.ConsumerGroup, stream)
				} else {
					c.log.Info().Msgf("connecting to stream %s with group %s resulted in status %s", stream, c.config.ConsumerGroup, status)
				}
			}

			// group established, now create consumer
			count, err := c.pool.XGroupCreateConsumer(context.Background(), stream, c.config.ConsumerGroup, id).Result()
			if err != nil {
				c.log.Error().Err(err).Msgf("could not create consumer %s on group %s", id, c.config.ConsumerGroup)
			} else {
				c.log.Info().Msgf("created consumer %s on stream %s with status %d", id, stream, count)
				streamArgs = append(streamArgs, stream, ">")
			}
		}
	}

	// now read the streams
	c.log.Info().Msgf("consumer established, reading streams...")
	for c.shutdown == false {
		// TODO: check for abandoned messages
		args := &redis.XReadGroupArgs{
			Group:    c.config.ConsumerGroup,
			Consumer: id,
			Count:    1,
			NoAck:    true,
			Block:    5 * time.Second,
			Streams:  streamArgs,
		}
		result, err := c.pool.XReadGroup(context.Background(), args).Result()
		if err != nil && err == redis.Nil { // redis.Nil means nothing was there and that is ok
			randomSleep() // little CPU saver (?)
		} else if err != nil {
			c.log.Error().Err(err).Msgf("error reading stream(s) for consumer %s", id)
			c.errorChannel <- err
		} else {
			for _, stream := range result {
				for _, msg := range stream.Messages {
					values := msg.Values
					msgData := values["data"]
					c.log.Info().Msgf("stream: %s, msg: %s", stream.Stream, msgData)
					event := &api.Event{}
					err = Decode(event, msgData.(string))
					if err != nil {
						c.log.Error().Err(err).Msgf("could not decode message")
						c.errorChannel <- err
					} else {
						c.eventChannel <- event
					}
				}
			}
		}
	}
	c.log.Info().Msgf("shutdown invoked for consumer %s", id)
}

func (c *Client) getConsumerId() string {
	// TODO: this is temp, make it more dynamic
	return c.config.ConsumerGroup + "_1"
}

func randomSleep() {
	s1 := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s1)
	var nap time.Duration
	nap = time.Duration(r.Intn(1000) + 10)
	time.Sleep(nap * time.Millisecond)
}

func (c *Client) connectToGrpcService(address string, service string) *grpc.ClientConn {
	c.log.Info().Msgf("Connecting to %s at %s", service, address)
	// common grpc retry opts
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithMax(3),
		grpc_retry.WithBackoff(grpc_retry.BackoffLinear(33 * time.Millisecond)),
		grpc_retry.WithCodes(codes.Unavailable, codes.Aborted),
	}
	clientInterceptors := []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(
			grpc_retry.UnaryClientInterceptor(retryOpts...),
		),
		grpc.WithChainStreamInterceptor(),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithTimeout(time.Duration(5) * time.Second),
	}
	eventConn, err := grpc.Dial(address, clientInterceptors...)
	if err != nil {
		log.Error().Err(err).Msgf("did not connect to %s", service)
		return nil
	} else {
		return eventConn
	}
}
