package dbwork

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gorm.io/gorm"
)

// DbOperation represents a database operation to be executed asynchronously
type DbOperation struct {
	Operation func(*gorm.DB) error
}

// BatchConfig defines configuration for batch processing
type BatchConfig struct {
	BatchSize      int           // Number of operations to collect before executing a transaction
	FlushInterval  time.Duration // Maximum time to wait before flushing a partial batch
	MaxRetries     int           // Maximum number of retries for failed transactions
	RetryDelayBase time.Duration // Base delay for retry backoff
}

// DefaultBatchConfig returns a default batch configuration
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		BatchSize:      100,
		FlushInterval:  5 * time.Second,
		MaxRetries:     3,
		RetryDelayBase: 100 * time.Millisecond,
	}
}

// DbWorkerPool manages a pool of workers for asynchronous database operations
type DbWorkerPool struct {
	queue         chan DbOperation
	workerNum     int
	db            *gorm.DB
	wg            sync.WaitGroup
	ctx           context.Context
	cancelFunc    context.CancelFunc
	batchConfig   BatchConfig
	statsInterval time.Duration
	statsEnabled  bool
	statsMutex    sync.Mutex
	stats         PoolStats
}

// PoolStats contains statistics about the worker pool
type PoolStats struct {
	OperationsProcessed int64
	BatchesProcessed    int64
	OperationsQueued    int
	FailedOperations    int64
	RetryAttempts       int64
}

// NewDbWorkerPool creates a new worker pool for database operations
func NewDbWorkerPool(db *gorm.DB, workerNum int, queueSize int, batchConfig BatchConfig) *DbWorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	pool := &DbWorkerPool{
		queue:         make(chan DbOperation, queueSize),
		workerNum:     workerNum,
		db:            db,
		ctx:           ctx,
		cancelFunc:    cancel,
		batchConfig:   batchConfig,
		statsInterval: 1 * time.Minute,
		statsEnabled:  true,
	}

	pool.Start()
	return pool
}

// Start launches the worker goroutines
func (p *DbWorkerPool) Start() {
	for i := 0; i < p.workerNum; i++ {
		p.wg.Add(1)
		go p.batchWorker(i)
	}

	// Start stats logger if enabled
	if p.statsEnabled {
		p.wg.Add(1)
		go p.statsLogger()
	}
}

// statsLogger periodically logs statistics about the worker pool
func (p *DbWorkerPool) statsLogger() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.statsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			queueLen := len(p.queue)

			// Get a snapshot of current stats with mutex protection
			p.statsMutex.Lock()
			stats := p.stats
			p.statsMutex.Unlock()

			fmt.Printf("[DbWorkerPool Stats] Queue: %d, Processed: %d, Batches: %d, Failed: %d, Retries: %d\n",
				queueLen, stats.OperationsProcessed, stats.BatchesProcessed,
				stats.FailedOperations, stats.RetryAttempts)
		}
	}
}

// batchWorker collects operations and processes them in batches
func (p *DbWorkerPool) batchWorker(id int) {
	defer p.wg.Done()

	batch := make([]DbOperation, 0, p.batchConfig.BatchSize)
	flushTicker := time.NewTicker(p.batchConfig.FlushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			// Process any remaining operations before exiting
			if len(batch) > 0 {
				p.processBatch(batch, id)
			}
			return

		case <-flushTicker.C:
			// Flush partial batch if there are any operations
			if len(batch) > 0 {
				p.processBatch(batch, id)
				batch = make([]DbOperation, 0, p.batchConfig.BatchSize)
			}

		case op, ok := <-p.queue:
			if !ok {
				// Channel closed, process remaining batch and exit
				if len(batch) > 0 {
					p.processBatch(batch, id)
				}
				return
			}

			// Add operation to batch
			batch = append(batch, op)

			// Process batch if it's full
			if len(batch) >= p.batchConfig.BatchSize {
				p.processBatch(batch, id)
				batch = make([]DbOperation, 0, p.batchConfig.BatchSize)
				// Reset the timer after processing a batch
				flushTicker.Reset(p.batchConfig.FlushInterval)
			}
		}
	}
}

// processBatch executes a batch of operations within a single transaction
func (p *DbWorkerPool) processBatch(batch []DbOperation, workerID int) {
	if len(batch) == 0 {
		return
	}

	batchSize := len(batch)
	fmt.Printf("Worker %d: Processing batch of %d operations\n", workerID, batchSize)

	// Track retry attempts for stats
	retryAttempts := int64(0)

	// Use transaction for the entire batch
	err := p.db.Transaction(func(tx *gorm.DB) error {
		// Use prepared statements for better performance
		txWithPrepare := tx.Session(&gorm.Session{PrepareStmt: true})

		for i, op := range batch {
			if err := op.Operation(txWithPrepare); err != nil {
				fmt.Printf("Worker %d: Operation %d/%d in batch failed: %v\n",
					workerID, i+1, batchSize, err)
				return err
			}
		}
		return nil
	})

	// Assume success by default, will set to false if all retries fail
	success := true

	// Retry logic for the entire batch
	if err != nil {
		success = false

		for retry := 0; retry < p.batchConfig.MaxRetries; retry++ {
			retryAttempts++

			// Wait with exponential backoff
			backoffTime := p.batchConfig.RetryDelayBase * time.Duration(1<<retry)
			time.Sleep(backoffTime)

			fmt.Printf("Worker %d: Retrying batch (attempt %d/%d) after %v\n",
				workerID, retry+1, p.batchConfig.MaxRetries, backoffTime)

			// Retry the transaction
			err = p.db.Transaction(func(tx *gorm.DB) error {
				txWithPrepare := tx.Session(&gorm.Session{PrepareStmt: true})

				for i, op := range batch {
					if err := op.Operation(txWithPrepare); err != nil {
						fmt.Printf("Worker %d: Operation %d/%d in retry batch failed: %v\n",
							workerID, i+1, batchSize, err)
						return err
					}
				}
				return nil
			})

			if err == nil {
				success = true
				break
			}
		}

		if !success {
			fmt.Printf("Worker %d: Batch processing failed after %d retries: %v\n",
				workerID, p.batchConfig.MaxRetries, err)
		}
	}

	// Update global stats with mutex protection
	p.statsMutex.Lock()
	p.stats.OperationsProcessed += int64(batchSize)
	p.stats.BatchesProcessed++
	p.stats.RetryAttempts += retryAttempts

	if !success {
		p.stats.FailedOperations += int64(batchSize)
	}
	p.statsMutex.Unlock()
}

// Submit adds a database operation to the queue
func (p *DbWorkerPool) Submit(op DbOperation) bool {
	select {
	case p.queue <- op:
		return true
	default:
		// Queue is full, operation is rejected
		return false
	}
}

// SubmitBlocking adds a database operation to the queue and blocks if the queue is full
func (p *DbWorkerPool) SubmitBlocking(op DbOperation) bool {
	select {
	case <-p.ctx.Done():
		return false
	case p.queue <- op:
		return true
	}
}

// Shutdown gracefully shuts down the worker pool
func (p *DbWorkerPool) Shutdown(wait bool) {
	p.cancelFunc()
	close(p.queue)

	if wait {
		p.wg.Wait()
	}
}

// SetStatsEnabled enables or disables statistics logging
func (p *DbWorkerPool) SetStatsEnabled(enabled bool) {
	p.statsEnabled = enabled
}

// SetStatsInterval sets the interval for statistics logging
func (p *DbWorkerPool) SetStatsInterval(interval time.Duration) {
	p.statsInterval = interval
}
