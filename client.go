package dedb_client_go

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/pocket5s/dedb-client-go/api"
)

type message struct {
	id     string
	stream string
}

type Client struct {
	config         ClientConfig
	server         api.DeDBClient
	pool           *redis.Client
	log            zerolog.Logger
	shutdown       bool
	streams        []string
	streamIds      map[string]string
	eventChannel   chan<- *api.Event
	errorChannel   chan<- error
	eventsReceived map[string]message
}

func NewClient(config ClientConfig) (*Client, error) {
	if config.Server == "" {
		return nil, fmt.Errorf("Server config entry required")
	}

	c := &Client{config: config}
	c.log = log.With().Str("logger", "DeDBClient").Logger()
	c.eventsReceived = make(map[string]message, 0)
	c.streamIds = make(map[string]string, 0)
	if len(config.StreamIds) == 0 {
		for _, v := range config.Streams {
			c.streamIds["dedb:stream:"+v] = "0-0"
		}
	} else {
		for k, v := range config.StreamIds {
			c.streamIds["dedb:stream:"+k] = v
		}
	}

	// if config.ConsumerGroup != "" {
	// c.log.Info().Msgf("setting up consumer group %s", config.ConsumerGroup)
	if len(config.Streams) == 0 {
		return nil, fmt.Errorf("Streams config entry required")
	}
	c.streams = make([]string, 0)
	for _, s := range config.Streams {
		c.streams = append(c.streams, "dedb:stream:"+s)
	}
	c.log.Info().Msgf("streams to process: %v", c.streams)
	c.eventChannel = config.EventChannel
	c.errorChannel = config.ErrorChannel
	// return nil, fmt.Errorf("ConsumerGroup config entry required")
	//}

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

func (c *Client) Ack(ctx context.Context, eventId string) {
	m, ok := c.eventsReceived[eventId]
	if ok {
		count, _ := c.pool.XAck(ctx, c.config.ConsumerGroup, m.stream, m.id).Result()
		if count != 1 {
			c.log.Warn().Msgf("ack not successful for stream %s, event id %s", m.stream, eventId)
		}
		delete(c.eventsReceived, eventId)
	}
}

func (c *Client) Connect(ctx context.Context) error {
	// connect to DeDB server
	c.log.Info().Msgf("connecting to DeDB server at %s", c.config.Server)
	for c.server == nil {
		if c.shutdown {
			return nil
		}
		conn := c.connectToGrpcService(c.config.Server, "dedb")
		if conn != nil {
			c.server = api.NewDeDBClient(conn)
			c.log.Info().Msg("connected to DeDB")
		} else {
			c.log.Warn().Msg("could not connect to DeDB server")
			time.Sleep(2 * time.Second)
		}
	}

	// connect to redis server
	// if c.config.ConsumerGroup != "" {
	pool, err := newPool(false, c.config, &c.log)
	if err != nil {
		return err
	}
	c.pool = pool
	c.log.Info().Msg("connected to redis")
	go c.listenForEvents()
	//}
	return nil
}

func (c *Client) Close() {
	c.shutdown = true
	if c.pool != nil {
		time.Sleep(7 * time.Second) // allow the event listener goroutine to shutdown
		c.pool.Close()
	}
}

func (c *Client) listenForEvents() {
	// see if there is a consumer group already established for each stream
	streamArgs := make([]string, 0)
	consumerId := ""
	if c.config.ConsumerGroup != "" {
		c.log.Info().Msgf("checking for consumer group %s on each stream requested", c.config.ConsumerGroup)
		consumerId = c.getConsumerId()
		for _, stream := range c.streams {
			groups, err := c.pool.XInfoGroups(context.Background(), stream).Result()
			if err != nil && err.Error() != "ERR no such key" {
				c.log.Error().Err(err).Msgf("could not query for consumer groups on stream %s", stream)
			} else {
				var found bool
				for _, g := range groups {
					if g.Name == "dedb:client:consumer_group:"+c.config.ConsumerGroup {
						found = true
					}
				}

				groupName := "dedb:client:consumer_group:" + c.config.ConsumerGroup
				if !found {
					c.log.Info().Msgf("consumer group %s not found, creating it", c.config.ConsumerGroup)
					status, err := c.pool.XGroupCreateMkStream(context.Background(), stream, groupName, "$").Result()
					if err != nil {
						c.log.Error().Err(err).Msgf("could not create group %s for stream %s", c.config.ConsumerGroup, stream)
					} else {
						c.log.Info().Msgf("connecting to stream %s with group %s resulted in status %s", stream, c.config.ConsumerGroup, status)
					}
				}

				// group established, now create consumer
				count, err := c.pool.XGroupCreateConsumer(context.Background(), stream, groupName, consumerId).Result()
				if err != nil {
					c.log.Error().Err(err).Msgf("could not create consumer %s on group %s", consumerId, c.config.ConsumerGroup)
				} else {
					c.log.Info().Msgf("created consumer %s on stream %s with status %d", consumerId, stream, count)
					// streamArgs = append(streamArgs, stream, ">")
					streamArgs = append(streamArgs, stream)
				}
			}
		}
	} else {
		streamArgs = c.streams
	}
	/*
		l := len(streamArgs)
			for i := 0; i < l; i++ {
				if c.config.StartStreamId == "" {
					streamArgs = append(streamArgs, ">")
				} else {
					streamArgs = append(streamArgs, c.config.StartStreamId)
				}
			}
	*/
	if consumerId != "" {
		c.readFromGroupStream(streamArgs, consumerId)
	} else {
		c.readFromStream()
	}
}

func (c *Client) readFromStream() {
	c.log.Info().Msgf("consumer established, reading streams %v", c.streams)
	// lastId := streamArgs[len(streamArgs)-1]
	streamArgs := make([]string, len(c.streams)*2)
	for i, k := range c.streams {
		streamArgs[i] = k
	}
	length := len(c.streams)
	for c.shutdown == false {
		for idx, val := range c.streams {
			streamArgs[idx+length] = c.streamIds[val]
		}

		// c.log.Info().Msgf("stream args: %v", streamArgs)
		args := &redis.XReadArgs{
			Count:   1,
			Block:   5 * time.Second,
			Streams: streamArgs,
		}

		if c.shutdown == false {
			result, err := c.pool.XRead(context.Background(), args).Result()
			if err != nil && err == redis.Nil {
				randomSleep()
			} else if err != nil {
				c.log.Error().Err(err).Msgf("error reading stream(s)")
				c.errorChannel <- err
			} else {
				for _, stream := range result {
					for _, msg := range stream.Messages {
						values := msg.Values
						msgData := values["data"]
						event := &api.Event{}
						err = Decode(event, msgData.(string))
						if err != nil {
							c.log.Error().Err(err).Msgf("could not decode message")
							c.errorChannel <- err
						} else {
							event.StreamId = msg.ID
							c.streamIds["dedb:stream:"+event.Domain] = msg.ID
							c.eventChannel <- event
							/* don't remember why I'm doing this...
							m := message{
								id:     msg.ID,
								stream: stream.Stream,
							}
							c.eventsReceived[event.Id] = m
							*/
						}
					}
				}
			}
		}
	}
	log.Info().Msg("shutdown invoked, stopping stream reads")
}

func (c *Client) readFromGroupStream(streamArgs []string, id string) {
	// now read the streams
	c.log.Info().Msgf("consumer established, reading streams...")
	for c.shutdown == false {
		// TODO: check for abandoned messages
		args := &redis.XReadGroupArgs{
			Group:    "dedb:client:consumer_group:" + c.config.ConsumerGroup,
			Consumer: id,
			Count:    1,
			NoAck:    true,
			Block:    5 * time.Second,
			Streams:  streamArgs,
		}
		// make sure shutdown was not called while waiting for block
		if c.shutdown == false {
			result, err := c.pool.XReadGroup(context.Background(), args).Result()
			if err == nil || err == redis.Nil { // redis.Nil means nothing was there and that is ok
				randomSleep() // little CPU saver (?)
			} else if err != nil {
				c.log.Error().Err(err).Msgf("error reading stream(s) for consumer %s", id)
				c.errorChannel <- err
			} else {
				for _, stream := range result {
					for _, msg := range stream.Messages {
						values := msg.Values
						msgData := values["data"]
						c.log.Debug().Msgf("stream: %s, msg: %s", stream.Stream, msgData)
						event := &api.Event{}
						err = Decode(event, msgData.(string))
						if err != nil {
							c.log.Error().Err(err).Msgf("could not decode message")
							c.errorChannel <- err
						} else {
							event.StreamId = msg.ID
							c.eventChannel <- event
							/*
								m := message{
									id:     msg.ID,
									stream: stream.Stream,
								}
								c.eventsReceived[event.Id] = m
							*/
						}
					}
				}
			}
		}
		go c.pingClientKey(id)
	}
	c.log.Info().Msgf("shutdown invoked for consumer %s", id)
	// timeout this client id to allow another instance to claim it
	c.pool.Expire(context.Background(), id, 1*time.Second)
}

func (c *Client) getConsumerId() string {
	var counter int
	var name string = c.config.ConsumerGroup
	for {
		counter++
		key := name + "_" + strconv.Itoa(counter)
		exists, err := c.pool.Exists(context.Background(), key).Result()
		if err != nil {
			c.log.Warn().Err(err).Msgf("could not determine if key %s exists", key)
			time.Sleep(5 * time.Second)
		} else if exists == 0 {
			c.pool.Set(context.Background(), key, "1", 60*time.Second)
			return key
		}
	}
}

func (c *Client) pingClientKey(key string) {
	c.pool.Expire(context.Background(), key, 60*time.Second)
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
