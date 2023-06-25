package context

import (
	"context"
	"mtcloud.com/mtstorage/pkg/discall/context/metadata"
)

// TIMEOUT metadata key definition
const (
	META_TIMEOUT   = "TIMEOUT"
	META_VERSION   = "VERSION"
	META_NAME      = "NAME"
	META_FROM      = "FROM"
	META_TO        = "TO"
	META_DOMAIN    = "DOMAIN"
	META_TRACK     = "TRACK"
	META_BROADCAST = "BROADCAST"
	META_TIMESTAMP = "TIMESTAMP"
	META_KEY       = "KEYS"

	META_SPANCTX = "SPANCONTEXT"
)

func SetMetadata(ctx context.Context, k, v string) context.Context {
	return metadata.Set(ctx, k, v)
}

func GetMetadata(ctx context.Context, k string) (string, bool) {
	return metadata.Get(ctx, k)
}
