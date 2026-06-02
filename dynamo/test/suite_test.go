package test

import (
	"testing"

	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/dynamo/mock"
	"github.com/stretchr/testify/suite"
)

// RepositorySuite is the Dynamo EventRepository unit test suite
type RepositorySuite struct {
	suite.Suite
	client *mock.Client
	repo   *dynamo.Repository
}

// Run the Dynamo EventRepository suite
func Test_RepositorySuite(t *testing.T) {
	suite.Run(t, new(RepositorySuite))
}

// Use the mocked TestDynamoClient to initialize the Dynamo EventRepository
func (s *RepositorySuite) SetupTest() {
	s.client = mock.NewClient()
	s.repo = dynamo.NewRepository(s.client, "test-events")
}
