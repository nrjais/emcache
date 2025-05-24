package coordinator

import (
	"context"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/mock_interfaces.go -package=mocks

// CoordinatorInterface defines the interface for the coordinator functionality
type CoordinatorInterface interface {
	Start(ctx context.Context)
}
