package teak

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cocosip/teak/internal/dispatch"
	"github.com/cocosip/teak/internal/store"
)

type factory struct {
	root    *store.Root
	options Options

	mu        sync.Mutex
	closed    bool
	closeDone chan struct{}
	closeErr  error
	logs      map[string]*logStream

	maintenanceCancel context.CancelFunc
	maintenanceDone   chan struct{}
	runGC             func(float64) error
	gcRuns            atomic.Uint64
	gcRewrites        atomic.Uint64
	gcNoRewrite       atomic.Uint64
	gcErrors          atomic.Uint64
	maintenancePanics atomic.Uint64
}

var _ Factory = (*factory)(nil)

// New opens a Teak factory using durable synchronous storage.
func New(options Options) (Factory, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	options.DefaultLog = normalizeLogConfig(options.DefaultLog)
	options.Logs = cloneLogConfigs(options.Logs)
	if options.ValueLogGCDiscardRatio == 0 {
		options.ValueLogGCDiscardRatio = 0.5
	}
	root, err := store.Open(options.Dir)
	if err != nil {
		return nil, err
	}
	maintenanceCtx, maintenanceCancel := context.WithCancel(context.Background())
	created := &factory{
		root:              root,
		options:           options,
		closeDone:         make(chan struct{}),
		logs:              make(map[string]*logStream),
		maintenanceCancel: maintenanceCancel,
		maintenanceDone:   make(chan struct{}),
		runGC:             root.RunValueLogGC,
	}
	if options.MaintenanceInterval > 0 {
		go created.maintenanceLoop(maintenanceCtx)
	} else {
		close(created.maintenanceDone)
	}
	return created, nil
}

func (f *factory) Open(ctx context.Context, name string) (Log, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, ErrClosed
	}
	if existing, ok := f.logs[name]; ok {
		return existing, nil
	}
	persistence, err := f.root.Stream(name)
	if err != nil {
		if f.options.Logger != nil {
			f.options.Logger.Error("teak storage operation failed", "operation", "open-log", "stream", name, "error", err)
		}
		return nil, mapError(err)
	}
	config := f.options.DefaultLog
	if override, ok := f.options.Logs[name]; ok {
		config = mergeLogConfig(override, f.options.DefaultLog)
	}
	scheduler, err := dispatch.New(persistence, dispatch.Config{
		PrefetchCapacity:  config.PrefetchCapacity,
		MaxInFlight:       config.MaxInFlight,
		VisibilityTimeout: config.VisibilityTimeout,
		RetryInitial:      config.RetryBackoff.Initial,
		RetryMax:          config.RetryBackoff.Max,
		RetryMultiplier:   config.RetryBackoff.Multiplier,
	})
	if err != nil {
		return nil, mapError(err)
	}
	stream := newLogStream(persistence, scheduler, f.options.Logger)
	f.logs[name] = stream
	return stream, nil
}

func (f *factory) Close(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	if f.closed {
		done := f.closeDone
		f.mu.Unlock()
		<-done
		return f.closeErr
	}
	f.closed = true
	logs := make([]*logStream, 0, len(f.logs))
	for _, stream := range f.logs {
		logs = append(logs, stream)
	}
	f.mu.Unlock()
	f.maintenanceCancel()
	<-f.maintenanceDone

	var closeErr error
	for _, stream := range logs {
		if err := stream.close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	if err := f.root.Close(); err != nil && closeErr == nil {
		closeErr = fmt.Errorf("teak: close storage: %w", err)
	}
	f.mu.Lock()
	f.closeErr = closeErr
	close(f.closeDone)
	f.mu.Unlock()
	return closeErr
}

func (f *factory) RunMaintenance(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	f.gcRuns.Add(1)
	err := f.runGC(f.options.ValueLogGCDiscardRatio)
	switch {
	case err == nil:
		f.gcRewrites.Add(1)
		return nil
	case errors.Is(err, store.ErrNoRewrite):
		f.gcNoRewrite.Add(1)
		return nil
	default:
		f.gcErrors.Add(1)
		if f.options.Logger != nil {
			f.options.Logger.Error("teak maintenance failed", "operation", "value-log-gc", "error", err)
		}
		return fmt.Errorf("teak: value-log GC: %w", err)
	}
}

func (f *factory) Stats() FactoryStats {
	return FactoryStats{
		ValueLogGCRuns:      f.gcRuns.Load(),
		ValueLogGCRewrites:  f.gcRewrites.Load(),
		ValueLogGCNoRewrite: f.gcNoRewrite.Load(),
		ValueLogGCErrors:    f.gcErrors.Load(),
		MaintenancePanics:   f.maintenancePanics.Load(),
	}
}

func (f *factory) maintenanceLoop(ctx context.Context) {
	defer close(f.maintenanceDone)
	ticker := time.NewTicker(f.options.MaintenanceInterval)
	defer ticker.Stop()
	f.maintenanceWorker(ctx, ticker.C)
}

func (f *factory) maintenanceWorker(ctx context.Context, ticks <-chan time.Time) {
	defer func() {
		if recovered := recover(); recovered != nil {
			f.maintenancePanics.Add(1)
			if f.options.Logger != nil {
				f.options.Logger.Error(
					"teak maintenance panic",
					"operation", "value-log-gc",
					"panic", recovered,
					"stack", string(debug.Stack()),
				)
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			if err := f.RunMaintenance(ctx); errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) {
				return
			}
		}
	}
}

func cloneLogConfigs(configs map[string]LogConfig) map[string]LogConfig {
	if configs == nil {
		return nil
	}
	cloned := make(map[string]LogConfig, len(configs))
	for name, config := range configs {
		cloned[name] = config
	}
	return cloned
}

func checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func mapError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, dispatch.ErrClosed), errors.Is(err, store.ErrClosed):
		return fmt.Errorf("%w: %v", ErrClosed, err)
	case errors.Is(err, dispatch.ErrInvalidCount), errors.Is(err, store.ErrInvalidBatchSize):
		return fmt.Errorf("%w: %v", ErrInvalidCount, err)
	case errors.Is(err, dispatch.ErrInvalidConfig):
		return fmt.Errorf("%w: %v", ErrInvalidOptions, err)
	case errors.Is(err, store.ErrInvalidStreamName):
		return fmt.Errorf("%w: %v", ErrInvalidName, err)
	case errors.Is(err, dispatch.ErrStaleDelivery):
		return fmt.Errorf("%w: %v", ErrStaleDelivery, err)
	case errors.Is(err, dispatch.ErrInvalidExtension):
		return fmt.Errorf("%w: %v", ErrInvalidExtension, err)
	case errors.Is(err, store.ErrNotFound):
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	case errors.Is(err, store.ErrBatchTooLarge):
		return fmt.Errorf("%w: %v", ErrBatchTooLarge, err)
	case errors.Is(err, store.ErrCorruptEnvelope), errors.Is(err, store.ErrUnsupportedEnvelope),
		errors.Is(err, store.ErrUnsupportedSchema), errors.Is(err, store.ErrTailRegression),
		errors.Is(err, store.ErrStateConflict):
		return fmt.Errorf("%w: %w", ErrCorruptStorage, err)
	default:
		return err
	}
}
