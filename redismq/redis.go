package redismq

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v9"
	"github.com/pkg/errors"
)

var redisVersionRE = regexp.MustCompile(`redis_version:(.+)`)

func redisPreflightChecks(client redis.UniversalClient) error {
	info, err := client.Info(context.Background(), "server").Result()
	if err != nil {
		return err
	}

	match := redisVersionRE.FindAllStringSubmatch(info, -1)
	if len(match) < 1 {
		return fmt.Errorf("could not extract redis version")
	}
	version := strings.TrimSpace(match[0][1])
	parts := strings.Split(version, ".")
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return err
	}
	if major < 5 {
		return fmt.Errorf("redis streams are not supported in version %q", version)
	}

	return nil
}

func incrementMessageID(id string) (string, error) {
	parts := strings.Split(id, "-")
	index := parts[1]
	parsed, err := strconv.ParseInt(index, 10, 64)
	if err != nil {
		return "", errors.Wrapf(err, "error parsing message ID %q", id)
	}
	return fmt.Sprintf("%s-%d", parts[0], parsed+1), nil
}
