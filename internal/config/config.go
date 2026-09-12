// Package config reads process configuration from environment variables,
// falling back to sane local-dev defaults so every binary still runs with
// zero setup.
package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// LoadDotEnv loads variables from a .env file in the current working
// directory into the process environment, if one exists. Variables already
// set in the environment take precedence over the file.
func LoadDotEnv() error {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("config: load .env: %w", err)
	}
	return nil
}

// String returns the env var named key, or def if it's unset or empty.
func String(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Int returns the env var named key parsed as an int, or def if it's unset,
// empty, or not a valid integer.
func Int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}

	return n
}
