package goripr

import (
	"strings"

	"github.com/google/uuid"
)

func generateUUID() string {
	uuid := uuid.New().String()

	return strings.Replace(uuid, "-", "", -1)
}
