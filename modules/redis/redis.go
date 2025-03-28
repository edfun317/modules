package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds Redis instance configuration
type Config struct {
	Name     string // Unique identifier for this Redis instance
	Addr     string // Redis server address (e.g., "localhost:6379")
	Password string // Redis password (if any)
	DB       int    // Redis database number
}

// Manager manages multiple Redis client instances
type Manager struct {
	clients map[string]*redis.Client
	mu      sync.RWMutex
}

// NewManager creates a new Redis manager
func NewManager() *Manager {
	return &Manager{
		clients: make(map[string]*redis.Client),
	}
}

// AddInstance adds a new Redis instance to the manager
func (m *Manager) AddInstance(cfg Config) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if instance already exists
	if _, exists := m.clients[cfg.Name]; exists {
		return fmt.Errorf("redis instance '%s' already exists", cfg.Name)
	}

	// Create new Redis client
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to redis instance '%s': %v", cfg.Name, err)
	}

	m.clients[cfg.Name] = client
	return nil
}

// RemoveInstance removes a Redis instance from the manager
func (m *Manager) RemoveInstance(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	client, exists := m.clients[name]
	if !exists {
		return fmt.Errorf("redis instance '%s' not found", name)
	}

	if err := client.Close(); err != nil {
		return fmt.Errorf("error closing redis instance '%s': %v", name, err)
	}

	delete(m.clients, name)
	return nil
}

// GetInstance returns a Redis client by its name
func (m *Manager) GetInstance(name string) (*redis.Client, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	client, exists := m.clients[name]
	if !exists {
		return nil, fmt.Errorf("redis instance '%s' not found", name)
	}

	return client, nil
}

// Set sets a key-value pair in the specified Redis instance
func (m *Manager) Set(ctx context.Context, instance string, key string, value interface{}, ttl time.Duration) error {
	client, err := m.GetInstance(instance)
	if err != nil {
		return err
	}

	return client.Set(ctx, key, value, ttl).Err()
}

// Get gets a value from the specified Redis instance
func (m *Manager) Get(ctx context.Context, instance string, key string) (string, error) {
	client, err := m.GetInstance(instance)
	if err != nil {
		return "", err
	}

	return client.Get(ctx, key).Result()
}

// Del deletes a key from the specified Redis instance
func (m *Manager) Del(ctx context.Context, instance string, key string) error {
	client, err := m.GetInstance(instance)
	if err != nil {
		return err
	}

	return client.Del(ctx, key).Err()
}

// Close closes all Redis client connections
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []string
	for name, client := range m.clients {
		if err := client.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to close '%s': %v", name, err))
		}
	}

	m.clients = make(map[string]*redis.Client)

	if len(errs) > 0 {
		return fmt.Errorf("errors closing redis instances: %v", errs)
	}
	return nil
}
