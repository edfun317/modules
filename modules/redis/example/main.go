package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/edfun317/modules/modules/redis"
)

func main() {
	// Create a new Redis manager
	manager := redis.NewManager()
	defer manager.Close()

	// Add multiple Redis instances
	instances := []redis.Config{
		{
			Name:     "cache",
			Addr:     "localhost:6379",
			Password: "",
			DB:       0,
		},
		{
			Name:     "session",
			Addr:     "localhost:6379",
			Password: "",
			DB:       1,
		},
	}

	// Add instances to the manager
	for _, cfg := range instances {
		if err := manager.AddInstance(cfg); err != nil {
			log.Printf("Failed to add instance '%s': %v", cfg.Name, err)
			continue
		}
		log.Printf("Added Redis instance: %s", cfg.Name)
	}

	ctx := context.Background()

	// Example: Using different instances
	if err := manager.Set(ctx, "cache", "user:1", "John Doe", time.Hour); err != nil {
		log.Printf("Failed to set in cache: %v", err)
	}

	if err := manager.Set(ctx, "session", "session:1", "abc123", time.Minute*30); err != nil {
		log.Printf("Failed to set in session: %v", err)
	}

	// Read from cache instance
	if value, err := manager.Get(ctx, "cache", "user:1"); err != nil {
		log.Printf("Failed to get from cache: %v", err)
	} else {
		fmt.Printf("Cache value: %s\n", value)
	}

	// Read from session instance
	if value, err := manager.Get(ctx, "session", "session:1"); err != nil {
		log.Printf("Failed to get from session: %v", err)
	} else {
		fmt.Printf("Session value: %s\n", value)
	}

	// Remove an instance
	if err := manager.RemoveInstance("session"); err != nil {
		log.Printf("Failed to remove session instance: %v", err)
	}

	// Try to use removed instance (should fail)
	if _, err := manager.Get(ctx, "session", "session:1"); err != nil {
		log.Printf("Expected error after removal: %v", err)
	}
}
