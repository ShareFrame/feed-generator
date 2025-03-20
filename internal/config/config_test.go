package config

import (
    "os"
    "strings"
    "testing"

    "github.com/spf13/pflag"
    "github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
    testCases := []struct {
        name          string
        sqsQueueURL   string
        homeEnv       string
        expectError   bool
        expectedError string
    }{
        {
            name:        "successful load",
            sqsQueueURL: "test-queue-url",
            expectError: false,
        },
        {
            name:          "missing SQS_QUEUE_URL",
            sqsQueueURL:   "",
            expectError:   true,
            expectedError: "SQS_QUEUE_URL environment variable not set",
        },
    }

    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            fs := pflag.NewFlagSet(t.Name(), pflag.ContinueOnError)

            err := fs.Parse(os.Args[1:])
            if err != nil && err != pflag.ErrHelp {
                if strings.HasPrefix(err.Error(), "flag provided but not defined: ") {
                    // Ignore testing flags
                } else {
                    t.Fatalf("Failed to parse flags: %v", err)
                }
            }

            if tc.sqsQueueURL != "" {
                os.Setenv("SQS_QUEUE_URL", tc.sqsQueueURL)
                defer os.Unsetenv("SQS_QUEUE_URL")
            } else {
                os.Unsetenv("SQS_QUEUE_URL")
            }

            if tc.homeEnv != "" {
                originalHome := os.Getenv("HOME")
                os.Setenv("HOME", tc.homeEnv)
                defer os.Setenv("HOME", originalHome)
            }

            cfg, err := LoadConfig(fs)

            if tc.expectError {
                assert.Error(t, err)

                if err != nil { // Add this check
                    assert.Nil(t, cfg)
                    if tc.expectedError != "" {
                        assert.EqualError(t, err, tc.expectedError)
                    } else {
                        assert.Contains(t, err.Error(), "unable to load SDK config")
                    }
                }
            } else {
                assert.NoError(t, err)
                assert.NotNil(t, cfg)
                assert.Equal(t, tc.sqsQueueURL, cfg.SQSQueueURL)
                assert.NotNil(t, cfg.SQSClient)
                assert.Equal(t, "wss://bsky.network", cfg.RelayHost)
            }
        })
    }
}