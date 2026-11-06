package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	RedisAddr        string
	RedisDB          int
	Workers          int
	QueueName        string
	Lease            time.Duration
	RecoveryInterval time.Duration
}

func Load() *Config {
	return LoadWithDefaults(Config{
		RedisAddr:        "localhost:6379",
		RedisDB:          0,
		Workers:          3,
		QueueName:        "default",
		Lease:            30 * time.Second,
		RecoveryInterval: 5 * time.Second,
	})
}

func LoadWithDefaults(defaults Config) *Config {
	return &Config{
		RedisAddr:        getEnv("REDIS_ADDR", defaults.RedisAddr),
		RedisDB:          getEnvAsInt("REDIS_DB", defaults.RedisDB),
		Workers:          getEnvAsInt("WORKERS", defaults.Workers),
		QueueName:        getEnv("QUEUE_NAME", defaults.QueueName),
		Lease:            getEnvAsDuration("LEASE", defaults.Lease),
		RecoveryInterval: getEnvAsDuration("RECOVERY_INTERVAL", defaults.RecoveryInterval),
	}
}

func getEnv(key, fallback string) string {
	if val, exists := os.LookupEnv(key); exists {
		return val
	}
	return fallback
}

func getEnvAsInt(key string, fallback int) int {
	if valStr, exists := os.LookupEnv(key); exists {
		if val, err := strconv.Atoi(valStr); err == nil {
			return val
		}
	}
	return fallback
}

func getEnvAsDuration(key string, fallback time.Duration) time.Duration {
	if valStr, exists := os.LookupEnv(key); exists {
		if val, err := time.ParseDuration(valStr); err == nil {
			return val
		}
	}
	return fallback
}
