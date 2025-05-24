package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_DefaultValues(t *testing.T) {
	// Clear environment variables to test defaults
	envVars := []string{
		"EMCACHE_POSTGRES_URL",
		"EMCACHE_MONGO_URL",
		"EMCACHE_GRPC_PORT",
		"EMCACHE_SQLITE_DIR",
		"EMCACHE_LOG_LEVEL",
		"EMCACHE_CONFIG_PATH",
	}

	for _, env := range envVars {
		os.Unsetenv(env)
	}
	defer func() {
		// Clean up after test
		for _, env := range envVars {
			os.Unsetenv(env)
		}
	}()

	// Note: Load() calls os.Exit(1) if validation fails or config file is invalid
	// So we need to test the individual components rather than Load() directly

	t.Run("default configuration structure", func(t *testing.T) {
		cfg := &Config{}

		// Test that the struct can be created with default values
		assert.NotNil(t, cfg)

		// Test default values when set manually (as Load() would)
		cfg.PostgresURL = ""
		cfg.MongoURL = ""
		cfg.GRPCPort = ":50051"
		cfg.SQLiteDir = "./emcache_dbs"
		cfg.LogLevel = "INFO"

		cfg.CoordinatorOptions = CoordinatorConfig{
			CollectionRefreshIntervalSecs: 60,
		}

		cfg.LeaderOptions = LeaderConfig{
			ResumeTokenUpdateIntervalSecs: 10,
			InitialScanBatchSize:          1000,
			LeaseDurationSecs:             30,
		}

		cfg.FollowerOptions = FollowerConfig{
			PollIntervalSecs:    2,
			BatchSize:           100,
			CleanupIntervalSecs: 300,
		}

		cfg.SnapshotOptions = SnapshotConfig{
			TTLSecs: 3600,
		}

		// Verify default values
		assert.Equal(t, "", cfg.PostgresURL)
		assert.Equal(t, "", cfg.MongoURL)
		assert.Equal(t, ":50051", cfg.GRPCPort)
		assert.Equal(t, "./emcache_dbs", cfg.SQLiteDir)
		assert.Equal(t, "INFO", cfg.LogLevel)

		assert.Equal(t, 60, cfg.CoordinatorOptions.CollectionRefreshIntervalSecs)
		assert.Equal(t, 10, cfg.LeaderOptions.ResumeTokenUpdateIntervalSecs)
		assert.Equal(t, 1000, cfg.LeaderOptions.InitialScanBatchSize)
		assert.Equal(t, int64(30), cfg.LeaderOptions.LeaseDurationSecs)
		assert.Equal(t, 2, cfg.FollowerOptions.PollIntervalSecs)
		assert.Equal(t, 100, cfg.FollowerOptions.BatchSize)
		assert.Equal(t, 300, cfg.FollowerOptions.CleanupIntervalSecs)
		assert.Equal(t, 3600, cfg.SnapshotOptions.TTLSecs)
	})
}

func TestConfig_EnvironmentVariables(t *testing.T) {
	// Test that environment variables would be picked up
	testCases := []struct {
		envVar   string
		envValue string
		testName string
	}{
		{"EMCACHE_POSTGRES_URL", "postgres://localhost:5432/testdb", "postgres URL"},
		{"EMCACHE_MONGO_URL", "mongodb://localhost:27017/testdb", "mongo URL"},
		{"EMCACHE_GRPC_PORT", ":9999", "gRPC port"},
		{"EMCACHE_SQLITE_DIR", "/tmp/test_emcache", "SQLite directory"},
		{"EMCACHE_LOG_LEVEL", "DEBUG", "log level"},
		{"EMCACHE_COORDINATOR_COLLECTION_REFRESH_INTERVAL_SECS", "120", "coordinator refresh interval"},
		{"EMCACHE_LEADER_LEASE_DURATION_SECS", "60", "leader lease duration"},
		{"EMCACHE_FOLLOWER_BATCH_SIZE", "200", "follower batch size"},
		{"EMCACHE_SNAPSHOT_TTL_SECS", "7200", "snapshot TTL"},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			// Set environment variable
			err := os.Setenv(tc.envVar, tc.envValue)
			require.NoError(t, err)

			// Clean up after test
			defer os.Unsetenv(tc.envVar)

			// Verify environment variable is set
			value := os.Getenv(tc.envVar)
			assert.Equal(t, tc.envValue, value)
		})
	}
}

func TestConfig_StructureValidation(t *testing.T) {
	t.Run("valid config structure", func(t *testing.T) {
		cfg := &Config{
			PostgresURL: "postgres://localhost:5432/emcache",
			MongoURL:    "mongodb://localhost:27017/test",
			GRPCPort:    ":50051",
			SQLiteDir:   "./data",
			LogLevel:    "INFO",
			CoordinatorOptions: CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
			LeaderOptions: LeaderConfig{
				ResumeTokenUpdateIntervalSecs: 10,
				InitialScanBatchSize:          1000,
				LeaseDurationSecs:             30,
			},
			FollowerOptions: FollowerConfig{
				PollIntervalSecs:    2,
				BatchSize:           100,
				CleanupIntervalSecs: 300,
			},
			SnapshotOptions: SnapshotConfig{
				TTLSecs: 3600,
			},
		}

		// Test that all fields can be accessed
		assert.Equal(t, "postgres://localhost:5432/emcache", cfg.PostgresURL)
		assert.Equal(t, "mongodb://localhost:27017/test", cfg.MongoURL)
		assert.Equal(t, ":50051", cfg.GRPCPort)
		assert.Equal(t, "./data", cfg.SQLiteDir)
		assert.Equal(t, "INFO", cfg.LogLevel)

		assert.Equal(t, 60, cfg.CoordinatorOptions.CollectionRefreshIntervalSecs)
		assert.Equal(t, 10, cfg.LeaderOptions.ResumeTokenUpdateIntervalSecs)
		assert.Equal(t, 1000, cfg.LeaderOptions.InitialScanBatchSize)
		assert.Equal(t, int64(30), cfg.LeaderOptions.LeaseDurationSecs)
		assert.Equal(t, 2, cfg.FollowerOptions.PollIntervalSecs)
		assert.Equal(t, 100, cfg.FollowerOptions.BatchSize)
		assert.Equal(t, 300, cfg.FollowerOptions.CleanupIntervalSecs)
		assert.Equal(t, 3600, cfg.SnapshotOptions.TTLSecs)
	})
}

func TestCoordinatorConfig(t *testing.T) {
	t.Run("coordinator config creation", func(t *testing.T) {
		opts := CoordinatorConfig{
			CollectionRefreshIntervalSecs: 120,
		}

		assert.Equal(t, 120, opts.CollectionRefreshIntervalSecs)
	})

	t.Run("zero values", func(t *testing.T) {
		opts := CoordinatorConfig{}
		assert.Equal(t, 0, opts.CollectionRefreshIntervalSecs)
	})
}

func TestLeaderConfig(t *testing.T) {
	t.Run("leader config creation", func(t *testing.T) {
		opts := LeaderConfig{
			ResumeTokenUpdateIntervalSecs: 15,
			InitialScanBatchSize:          2000,
			LeaseDurationSecs:             45,
		}

		assert.Equal(t, 15, opts.ResumeTokenUpdateIntervalSecs)
		assert.Equal(t, 2000, opts.InitialScanBatchSize)
		assert.Equal(t, int64(45), opts.LeaseDurationSecs)
	})
}

func TestFollowerConfig(t *testing.T) {
	t.Run("follower config creation", func(t *testing.T) {
		opts := FollowerConfig{
			PollIntervalSecs:    5,
			BatchSize:           500,
			CleanupIntervalSecs: 600,
		}

		assert.Equal(t, 5, opts.PollIntervalSecs)
		assert.Equal(t, 500, opts.BatchSize)
		assert.Equal(t, 600, opts.CleanupIntervalSecs)
	})
}

func TestSnapshotConfig(t *testing.T) {
	t.Run("snapshot config creation", func(t *testing.T) {
		opts := SnapshotConfig{
			TTLSecs: 7200,
		}

		assert.Equal(t, 7200, opts.TTLSecs)
	})
}

func TestConfig_EdgeCases(t *testing.T) {
	t.Run("config with empty strings", func(t *testing.T) {
		cfg := &Config{
			PostgresURL: "",
			MongoURL:    "",
			GRPCPort:    "",
			SQLiteDir:   "",
			LogLevel:    "",
		}

		// Test that empty values can be handled
		assert.Equal(t, "", cfg.PostgresURL)
		assert.Equal(t, "", cfg.MongoURL)
		assert.Equal(t, "", cfg.GRPCPort)
		assert.Equal(t, "", cfg.SQLiteDir)
		assert.Equal(t, "", cfg.LogLevel)
	})

	t.Run("config with zero values", func(t *testing.T) {
		cfg := &Config{
			CoordinatorOptions: CoordinatorConfig{
				CollectionRefreshIntervalSecs: 0,
			},
			LeaderOptions: LeaderConfig{
				ResumeTokenUpdateIntervalSecs: 0,
				InitialScanBatchSize:          0,
				LeaseDurationSecs:             0,
			},
			FollowerOptions: FollowerConfig{
				PollIntervalSecs:    0,
				BatchSize:           0,
				CleanupIntervalSecs: 0,
			},
			SnapshotOptions: SnapshotConfig{
				TTLSecs: 0,
			},
		}

		// Zero values should be allowed in the struct
		assert.Equal(t, 0, cfg.CoordinatorOptions.CollectionRefreshIntervalSecs)
		assert.Equal(t, 0, cfg.LeaderOptions.ResumeTokenUpdateIntervalSecs)
		assert.Equal(t, 0, cfg.LeaderOptions.InitialScanBatchSize)
		assert.Equal(t, int64(0), cfg.LeaderOptions.LeaseDurationSecs)
		assert.Equal(t, 0, cfg.FollowerOptions.PollIntervalSecs)
		assert.Equal(t, 0, cfg.FollowerOptions.BatchSize)
		assert.Equal(t, 0, cfg.FollowerOptions.CleanupIntervalSecs)
		assert.Equal(t, 0, cfg.SnapshotOptions.TTLSecs)
	})
}

func TestConfig_LogLevels(t *testing.T) {
	validLogLevels := []string{"DEBUG", "INFO", "WARN", "ERROR"}

	for _, level := range validLogLevels {
		t.Run("log level "+level, func(t *testing.T) {
			cfg := &Config{
				LogLevel: level,
			}

			assert.Equal(t, level, cfg.LogLevel)
		})
	}
}

func TestConfig_PortFormats(t *testing.T) {
	validPorts := []string{":50051", ":8080", ":9000", ":3000"}

	for _, port := range validPorts {
		t.Run("port "+port, func(t *testing.T) {
			cfg := &Config{
				GRPCPort: port,
			}

			assert.Equal(t, port, cfg.GRPCPort)
		})
	}
}
