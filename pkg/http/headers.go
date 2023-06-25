package http

// Standard S3 HTTP response constants
const (
	LastModified       = "Last-Modified"
	Date               = "Date"
	ETag               = "ETag"
	ContentType        = "Content-Type"
	ContentMD5         = "Content-Md5"
	ContentEncoding    = "Content-Encoding"
	Expires            = "Expires"
	ContentLength      = "Content-Length"
	ContentLanguage    = "Content-Language"
	ContentRange       = "Content-Range"
	Connection         = "Connection"
	AcceptRanges       = "Accept-Ranges"
	AmzBucketRegion    = "X-Amz-Bucket-Region"
	ServerInfo         = "Server"
	RetryAfter         = "Retry-After"
	Location           = "Location"
	CacheControl       = "Cache-Control"
	ContentDisposition = "Content-Disposition"
	Authorization      = "Authorization"
	Action             = "Action"
	Range              = "Range"
)

// Standard S3 HTTP request constants
const (
	IfUnmodifiedSince = "If-Unmodified-Since"
	IfNoneMatch       = "If-None-Match"

	// Server-Status
	MinIOServerStatus = "x-minio-server-status"

	// Header indicates permanent delete replication status.
	MinIODeleteReplicationStatus = "X-Minio-Replication-Delete-Status"
	// Header indicates delete-marker replication status.
	MinIODeleteMarkerReplicationStatus = "X-Minio-Replication-DeleteMarker-Status"
	// Header indicates if its a GET/HEAD proxy request for active-active replication
	MinIOSourceProxyRequest = "X-Minio-Source-Proxy-Request"
	// Header indicates that this request is a replication request to create a REPLICA
	MinIOSourceReplicationRequest = "X-Minio-Source-Replication-Request"
	// Header indicates replication reset status.
	MinIOReplicationResetStatus = "X-Minio-Replication-Reset-Status"

	// predicted date/time of transition
	MinIOTransition = "X-Minio-Transition"
)

// Common http query params S3 API
const (
	VersionID = "versionId"

	PartNumber = "partNumber"

	UploadID = "uploadId"
)
