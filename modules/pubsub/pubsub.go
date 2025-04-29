package pubsub

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Client struct {
	subscriber *googlecloud.Subscriber
	publisher  *googlecloud.Publisher
	options    *Options
}

func NewClient(opts ...Option) (*Client, error) {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	publisher, err := googlecloud.NewPublisher(
		googlecloud.PublisherConfig{
			ProjectID: options.ProjectID,
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	subscriber, err := googlecloud.NewSubscriber(
		googlecloud.SubscriberConfig{
			ProjectID:                options.ProjectID,
			GenerateSubscriptionName: options.SubscriptionNameFn,
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscriber: %w", err)
	}

	return &Client{
		publisher:  publisher,
		subscriber: subscriber,
		options:    options,
	}, nil
}

func (c *Client) Publish(topic string, msgs ...*message.Message) error {
	return c.publisher.Publish(topic, msgs...)
}

func (c *Client) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return c.subscriber.Subscribe(ctx, topic)
}

func (c *Client) Close() error {
	if err := c.publisher.Close(); err != nil {
		return fmt.Errorf("failed to close publisher: %w", err)
	}
	if err := c.subscriber.Close(); err != nil {
		return fmt.Errorf("failed to close subscriber: %w", err)
	}
	return nil
}
