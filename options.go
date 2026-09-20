package teak

import (
	"fmt"
	"log/slog"
	"time"
)

const (
	defaultPrefetchCapacity = 1024
	defaultMaxInFlight      = 1024
	defaultVisibility       = 30 * time.Second
	defaultRetryInitial     = time.Second
	defaultRetryMax         = time.Minute
	defaultRetryMultiplier  = 2
)

// Options configures a Teak factory without exposing Badger internals.
type Options struct {
	Dir        string
	DefaultLog LogConfig
	Logs       map[string]LogConfig
	Logger     *slog.Logger
}

// LogConfig controls one stream's bounded delivery scheduler.
type LogConfig struct {
	PrefetchCapacity  int
	MaxInFlight       int
	VisibilityTimeout time.Duration
	RetryBackoff      BackoffConfig
}

// BackoffConfig controls delayed redelivery after Retry.
type BackoffConfig struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
}

// DefaultOptions returns the recommended durable queue configuration.
func DefaultOptions(dir string) Options {
	return Options{Dir: dir, DefaultLog: DefaultLogConfig()}
}

// WithDir returns a copy configured to use dir.
func (o Options) WithDir(dir string) Options {
	o.Dir = dir
	return o
}

// WithLogger returns a copy configured with logger. A nil logger disables logging.
func (o Options) WithLogger(logger *slog.Logger) Options {
	o.Logger = logger
	return o
}

// WithDefaultLog returns a copy configured with the default stream settings.
func (o Options) WithDefaultLog(config LogConfig) Options {
	o.DefaultLog = config
	return o
}

// WithLog returns a copy with a named stream override. Existing maps are copied.
func (o Options) WithLog(name string, config LogConfig) Options {
	logs := make(map[string]LogConfig, len(o.Logs)+1)
	for existingName, existingConfig := range o.Logs {
		logs[existingName] = existingConfig
	}
	logs[name] = config
	o.Logs = logs
	return o
}

// DefaultLogConfig returns the recommended per-stream limits and timing.
func DefaultLogConfig() LogConfig {
	return LogConfig{
		PrefetchCapacity:  defaultPrefetchCapacity,
		MaxInFlight:       defaultMaxInFlight,
		VisibilityTimeout: defaultVisibility,
		RetryBackoff: BackoffConfig{
			Initial:    defaultRetryInitial,
			Max:        defaultRetryMax,
			Multiplier: defaultRetryMultiplier,
		},
	}
}

// WithPrefetchCapacity returns a copy with the fresh-record capacity set.
func (c LogConfig) WithPrefetchCapacity(capacity int) LogConfig {
	c.PrefetchCapacity = capacity
	return c
}

// WithMaxInFlight returns a copy with the delivery lease limit set.
func (c LogConfig) WithMaxInFlight(maximum int) LogConfig {
	c.MaxInFlight = maximum
	return c
}

// WithVisibilityTimeout returns a copy with the delivery visibility timeout set.
func (c LogConfig) WithVisibilityTimeout(timeout time.Duration) LogConfig {
	c.VisibilityTimeout = timeout
	return c
}

// WithRetryBackoff returns a copy with retry timing set.
func (c LogConfig) WithRetryBackoff(backoff BackoffConfig) LogConfig {
	c.RetryBackoff = backoff
	return c
}

// WithInitial returns a copy with the initial retry delay set.
func (b BackoffConfig) WithInitial(initial time.Duration) BackoffConfig {
	b.Initial = initial
	return b
}

// WithMax returns a copy with the maximum retry delay set.
func (b BackoffConfig) WithMax(maximum time.Duration) BackoffConfig {
	b.Max = maximum
	return b
}

// WithMultiplier returns a copy with the exponential retry multiplier set.
func (b BackoffConfig) WithMultiplier(multiplier float64) BackoffConfig {
	b.Multiplier = multiplier
	return b
}

func normalizeLogConfig(config LogConfig) LogConfig {
	defaults := DefaultLogConfig()
	if config.PrefetchCapacity == 0 {
		config.PrefetchCapacity = defaults.PrefetchCapacity
	}
	if config.MaxInFlight == 0 {
		config.MaxInFlight = defaults.MaxInFlight
	}
	if config.VisibilityTimeout == 0 {
		config.VisibilityTimeout = defaults.VisibilityTimeout
	}
	if config.RetryBackoff.Initial == 0 {
		config.RetryBackoff.Initial = defaults.RetryBackoff.Initial
	}
	if config.RetryBackoff.Max == 0 {
		config.RetryBackoff.Max = defaults.RetryBackoff.Max
	}
	if config.RetryBackoff.Multiplier == 0 {
		config.RetryBackoff.Multiplier = defaults.RetryBackoff.Multiplier
	}
	return config
}

func validateOptions(options Options) error {
	if options.Dir == "" {
		return fmt.Errorf("%w: directory is required", ErrInvalidOptions)
	}
	if err := validateLogConfig(normalizeLogConfig(options.DefaultLog)); err != nil {
		return err
	}
	for name, config := range options.Logs {
		if err := validateLogConfig(mergeLogConfig(config, normalizeLogConfig(options.DefaultLog))); err != nil {
			return fmt.Errorf("teak: invalid configuration for log %q: %w", name, err)
		}
	}
	return nil
}

func mergeLogConfig(config, base LogConfig) LogConfig {
	if config.PrefetchCapacity == 0 {
		config.PrefetchCapacity = base.PrefetchCapacity
	}
	if config.MaxInFlight == 0 {
		config.MaxInFlight = base.MaxInFlight
	}
	if config.VisibilityTimeout == 0 {
		config.VisibilityTimeout = base.VisibilityTimeout
	}
	if config.RetryBackoff.Initial == 0 {
		config.RetryBackoff.Initial = base.RetryBackoff.Initial
	}
	if config.RetryBackoff.Max == 0 {
		config.RetryBackoff.Max = base.RetryBackoff.Max
	}
	if config.RetryBackoff.Multiplier == 0 {
		config.RetryBackoff.Multiplier = base.RetryBackoff.Multiplier
	}
	return config
}

func validateLogConfig(config LogConfig) error {
	if config.PrefetchCapacity <= 0 || config.MaxInFlight <= 0 || config.VisibilityTimeout <= 0 ||
		config.RetryBackoff.Initial < 0 || config.RetryBackoff.Max < config.RetryBackoff.Initial ||
		config.RetryBackoff.Multiplier < 1 {
		return ErrInvalidOptions
	}
	return nil
}
