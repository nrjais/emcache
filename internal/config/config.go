package config

import (
	"log"
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
		log.Printf("Attempting to load configuration from specified file: %s", configFile)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("./config")
		v.AddConfigPath("/etc/emcache/")
		log.Printf("EMCACHE_CONFIG_PATH not set, looking for 'config.yaml' in default paths [., ./config, /etc/emcache/]")
	}

	err := v.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Println("Warning: Config file not found. Using defaults and environment variables.")
		} else {
			log.Fatalf("Failed to read config file: %v", err)
		}
	} else {
		log.Printf("Configuration loaded from file: %s", v.ConfigFileUsed())
	}

	var cfg Config
	err = v.Unmarshal(&cfg)
	if err != nil {
		log.Fatalf("Failed to parse configuration: %v", err)
	}

	validateConfig(&cfg)
	logConfig(&cfg)
	return &cfg
}

func validateConfig(cfg *Config) {
	validator := validator.New()

	err := validator.Struct(cfg)
	if err != nil {
		log.Fatalf("Config validation failed: %v", err)
	}
	log.Println("Configuration validated successfully.")
}

func logConfig(cfg *Config) {
	log.Println("Final Configuration:")
	log.Printf(" - Postgres URL: %s", cfg.PostgresURL)
	log.Printf(" - Mongo URL: %s", cfg.MongoURL)
	log.Printf(" - gRPC Port: %s", cfg.GRPCPort)
	log.Printf(" - SQLite Dir: %s", cfg.SQLiteDir)
	log.Printf(" - Log Level: %s", cfg.LogLevel)
	log.Printf(" - Coordinator Options: %+v", cfg.CoordinatorOptions)
	log.Printf(" - Leader Options: %+v", cfg.LeaderOptions)
	log.Printf(" - Follower Options: %+v", cfg.FollowerOptions)
	log.Printf(" - Snapshot Options: %+v", cfg.SnapshotOptions)
}
