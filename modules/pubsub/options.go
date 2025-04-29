package pubsub

import (
	"fmt"
)

type Options struct {
	ProjectID          string
	SubscriptionNameFn func(topic string) string
}

type Option func(*Options)

func defaultOptions() *Options {
	return &Options{
		SubscriptionNameFn: func(topic string) string {
			return fmt.Sprintf("sub_%s", topic)
		},
	}
}

func WithProjectID(projectID string) Option {
	return func(o *Options) {
		o.ProjectID = projectID
	}
}

func WithSubscriptionNameFn(fn func(topic string) string) Option {
	return func(o *Options) {
		o.SubscriptionNameFn = fn
	}
}
