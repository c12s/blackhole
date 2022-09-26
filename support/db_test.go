package support

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_GetConfigFromEnv(t *testing.T) {
	t.Run("Some env variables are missing", func(t *testing.T) {
		os.Setenv("DB_USERNAME", "test")
		os.Setenv("DB_PASSWORD", "test")
		os.Setenv("DB_NAME", "test")

		_, err := getDBConfigFromEnv()
		assert.NotNil(t, err)

		os.Setenv("DB_USERNAME", "")
		os.Setenv("DB_PASSWORD", "")
		os.Setenv("DB_NAME", "")
	})

	t.Run("All env variables are present", func(t *testing.T) {
		os.Setenv("DB_USERNAME", "test")
		os.Setenv("DB_PASSWORD", "test")
		os.Setenv("DB_NAME", "test")
		os.Setenv("DB_HOST", "test")
		os.Setenv("DB_PORT", "test")

		_, err := getDBConfigFromEnv()
		assert.Nil(t, err)
	})

}
