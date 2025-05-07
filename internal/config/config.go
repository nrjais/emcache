package config

import (
	"log/slog"
	"os"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

type Config struct {
	PostgresURL        string            `mapstructure:"postgres_url" validate:"required"`
	MongoURL           string            `mapstructure:"mongo_url" validate:"required"`
	GRPCPort           string            `mapstructure:"grpc_port" validate:"required"`
	SQLiteDir          string            `mapstructure:"sqlite_dir" validate:"required"`
	LogLevel           string            `mapstructure:"log_level" validate:"required,uppercase"`
	CoordinatorOptions CoordinatorConfig `mapstructure:"coordinator" validate:"required"`
	LeaderOptions      LeaderConfig      `mapstructure:"leader" validate:"required"`
	FollowerOptions    FollowerConfig    `mapstructure:"follower" validate:"required"`
	SnapshotOptions    SnapshotConfig    `mapstructure:"snapshot" validate:"required"`
}

type CoordinatorConfig struct {
	CollectionRefreshIntervalSecs int `mapstructure:"collection_refresh_interval_secs" validate:"min=1"`
}

type LeaderConfig struct {
	ResumeTokenUpdateIntervalSecs int   `mapstructure:"resume_token_update_interval_secs" validate:"min=1"`
	InitialScanBatchSize          int   `mapstructure:"initial_scan_batch_size" validate:"min=1"`
	LeaseDurationSecs             int64 `mapstructure:"lease_duration_secs" validate:"min=1"`
}

type FollowerConfig struct {
	PollIntervalSecs    int `mapstructure:"poll_interval_secs" validate:"min=1"`
	BatchSize           int `mapstructure:"batch_size" validate:"min=1"`
	CleanupIntervalSecs int `mapstructure:"cleanup_interval_secs" validate:"min=1"`
}

type SnapshotConfig struct {
	TTLSecs int `mapstructure:"ttl_secs" validate:"min=1"`
}

func Load() *Config {
	v := viper.New()

	v.SetDefault("postgres_url", "")
	v.SetDefault("mongo_url", "")
	v.SetDefault("grpc_port", ":50051")
	v.SetDefault("sqlite_dir", "./emcache_dbs")
	v.SetDefault("log_level", "INFO")
	v.SetDefault("coordinator.collection_refresh_interval_secs", 60)
	v.SetDefault("leader.resume_token_update_interval_secs", 10)
	v.SetDefault("leader.initial_scan_batch_size", 1000)
	v.SetDefault("leader.lease_duration_secs", 30)
	v.SetDefault("follower.poll_interval_secs", 2)
	v.SetDefault("follower.batch_size", 100)
	v.SetDefault("follower.cleanup_interval_secs", 300)
	v.SetDefault("snapshot.ttl_secs", 3600)

	v.SetEnvPrefix("EMCACHE")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	configFile := os.Getenv("EMCACHE_CONFIG_PATH")
	if configFile != "" {
		v.SetConfigFile(configFile)
		slog.Info("Loading configuration from specified file", "path", configFile)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("./config")
		v.AddConfigPath("/etc/emcache/")
		slog.Info("Config path not set, using default paths",
			"paths", []string{".", "./config", "/etc/emcache/"},
			"filename", "config.yaml")
	}

	err := v.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			slog.Warn("Config file not found, using defaults and environment variables")
		} else {
			slog.Error("Failed to read config file", "error", err)
			os.Exit(1)
		}
	} else {
		slog.Info("Configuration loaded", "file", v.ConfigFileUsed())
	}

	var cfg Config
	err = v.Unmarshal(&cfg)
	if err != nil {
		slog.Error("Failed to parse configuration", "error", err)
		os.Exit(1)
	}

	validateConfig(&cfg)
	logConfig(&cfg)
	return &cfg
}

func validateConfig(cfg *Config) {
	validator := validator.New()

	err := validator.Struct(cfg)
	if err != nil {
		slog.Error("Config validation failed", "error", err)
		os.Exit(1)
	}
	slog.Info("Configuration validated successfully")
}

func logConfig(cfg *Config) {
	slog.Info("Final Configuration",
		"postgres_url", cfg.PostgresURL,
		"mongo_url", cfg.MongoURL,
		"grpc_port", cfg.GRPCPort,
		"sqlite_dir", cfg.SQLiteDir,
		"log_level", cfg.LogLevel,
		"coordinator", cfg.CoordinatorOptions,
		"leader", cfg.LeaderOptions,
		"follower", cfg.FollowerOptions,
		"snapshot", cfg.SnapshotOptions)
}
