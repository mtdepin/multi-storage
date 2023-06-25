package storageerror

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
)

// APIError structure
type APIError struct {
	Code           string
	Description    string
	HTTPStatusCode int
}

// APIErrorResponse - storageerror response format
type APIErrorResponse struct {
	XMLName    xml.Name `xml:"Error" json:"-"`
	Code       string
	Message    string
	Key        string `xml:"Key,omitempty" json:"Key,omitempty"`
	BucketName string `xml:"BucketName,omitempty" json:"BucketName,omitempty"`
	Resource   string
	Region     string `xml:"Region,omitempty" json:"Region,omitempty"`
	RequestID  string `xml:"RequestId" json:"RequestId"`
	HostID     string `xml:"HostId" json:"HostId"`
}

// APIErrorCode type of storageerror status.
type APIErrorCode int

//go:generate stringer -type=APIErrorCode -trimprefix=Err $GOFILE

// Error codes, non exhaustive list - http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
const (
	ErrNone APIErrorCode = iota
	ErrAccessDenied
	ErrBadDigest
	ErrEntityTooSmall
	ErrEntityTooLarge
	ErrPolicyTooLarge
	ErrIncompleteBody
	ErrInvalidDigest
	ErrInvalidArguments
	ErrInternalError
	ErrInvalidRequestBody
	ErrInvalidCopySource
	ErrInvalidMetadataDirective
	ErrInvalidCopyDest

	ErrInvalidStorageClass
	ErrAdminInvalidArgument

	ErrClientDisconnected

	ErrInvalidObjectName
	ErrInvalidObjectNamePrefixSlash

	ErrNoSuchBucket
	ErrBucketNotEmpty
	ErrBucketAlreadyOwnedByYou
	ErrNoSuchBucketPolicy
	ErrNoSuchLifecycleConfiguration
	ErrBucketTaggingNotFound
	ErrNoSuchLoggingConfiguration
	ErrNoSuchACLConfiguration

	ErrObjectTaggingNotFound
	ErrNoSuchKey

	ErrWriteDatabaseFailed
	ErrInvalidRequest
)

type errorCodeMap map[APIErrorCode]APIError

func (e errorCodeMap) ToAPIErrWithErr(errCode APIErrorCode, err error) APIError {
	apiErr, ok := e[errCode]
	if !ok {
		apiErr = e[ErrInternalError]
	}
	if err != nil {
		apiErr.Description = fmt.Sprintf("%s (%s)", apiErr.Description, err)
	}

	return apiErr
}

func (e errorCodeMap) ToAPIErr(errCode APIErrorCode) APIError {
	return e.ToAPIErrWithErr(errCode, nil)
}

// ErrorCodes storageerror code to APIError structure, these fields carry respective
// descriptions for all the storageerror responses.
var ErrorCodes = errorCodeMap{
	ErrInvalidCopyDest: {
		Code:           "InvalidRequest",
		Description:    "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidArguments: {
		Code:           "InvalidArgument",
		Description:    "Copy Source must mention the source bucket and key: sourcebucket/sourcekey.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidCopySource: {
		Code:           "InvalidArgument",
		Description:    "Copy Source must mention the source bucket and key: sourcebucket/sourcekey.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMetadataDirective: {
		Code:           "InvalidArgument",
		Description:    "Unknown metadata directive.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidStorageClass: {
		Code:           "InvalidStorageClass",
		Description:    "Invalid storage class.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRequestBody: {
		Code:           "InvalidArgument",
		Description:    "Body shouldn't be set for this request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRequest: {
		Code:           "InvalidRequest",
		Description:    "Invalid Request",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInternalError: {
		Code:           "InternalError",
		Description:    "internal storageerror",
		HTTPStatusCode: http.StatusInternalServerError,
	},
	ErrInvalidDigest: {
		Code:           "InvalidDigest",
		Description:    "The Content-Md5 you specified is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrIncompleteBody: {
		Code:           "IncompleteBody",
		Description:    "You did not provide the number of bytes specified by the Content-Length HTTP header.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNoSuchBucket: {
		Code:           "NoSuchBucket",
		Description:    "Bucket not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrBucketNotEmpty: {
		Code:           "BucketNotEmpty",
		Description:    "The bucket you tried to delete is not empty",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrBucketAlreadyOwnedByYou: {
		Code:           "BucketAlreadyOwnedByYou",
		Description:    "Your previous request to create the named bucket succeeded and you already own it.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrNoSuchBucketPolicy: {
		Code:           "NoSuchBucketPolicy",
		Description:    "The bucket policy does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchLifecycleConfiguration: {
		Code:           "NoSuchLifecycleConfiguration",
		Description:    "The lifecycle configuration does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrBucketTaggingNotFound: {
		Code:           "NoSuchTagSet",
		Description:    "The TagSet does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchLoggingConfiguration: {
		Code:           "NoSuchLogSet",
		Description:    "The LogSet does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchACLConfiguration: {
		Code:           "NoSuchAclSet",
		Description:    "The AclSet does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},

	ErrWriteDatabaseFailed: {
		Code:           "WriteDatabaseFailed",
		Description:    "write database failed",
		HTTPStatusCode: http.StatusConflict,
	},

	ErrNoSuchKey: {
		Code:           "NoSuchKey",
		Description:    "Object not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrObjectTaggingNotFound: {
		Code:           "NoSuchObjectTagSet",
		Description:    "The TagSet does not exist",
		HTTPStatusCode: http.StatusNotFound,
	},
	// Add your storageerror structure here.
}

// toAPIErrorCode - Converts embedded errors. Convenience
// function written to handle all cases where we have known types of
// errors returned by underlying layers.
func toAPIErrorCode(ctx context.Context, err error) (apiErr APIErrorCode) {
	if err == nil {
		return ErrNone
	}

	// Only return ErrClientDisconnected if the provided context is actually canceled.
	// This way downstream context.Canceled will still report ErrOperationTimedOut
	select {
	case <-ctx.Done():
		if ctx.Err() == context.Canceled {
			return ErrClientDisconnected
		}
	default:
	}

	switch err {
	case ErrInvalidArgument:
		apiErr = ErrAdminInvalidArgument
	case errDataTooLarge:
		apiErr = ErrEntityTooLarge
	case errDataTooSmall:
		apiErr = ErrEntityTooSmall
	case errInvalidStorageClass:
		apiErr = ErrInvalidStorageClass
	}

	if apiErr != ErrNone {
		// If there was a match in the above switch case.
		return apiErr
	}

	switch err.(type) {
	case BucketNotEmpty:
		apiErr = ErrBucketNotEmpty
	case BucketNotFound:
		apiErr = ErrNoSuchBucket
	case WriteDataBaseFailed:
		apiErr = ErrWriteDatabaseFailed
	case BucketACLNotFound:
		apiErr = ErrNoSuchACLConfiguration
	case BucketAlreadyOwnedByYou:
		apiErr = ErrBucketAlreadyOwnedByYou
	case BucketPolicyNotFound:
		apiErr = ErrNoSuchBucketPolicy
	case ObjectNotFound:
		apiErr = ErrNoSuchKey
	case ObjectTaggingNotFound:
		apiErr = ErrObjectTaggingNotFound
	}
	return apiErr
}

var noError = APIError{}

// ToAPIError - Converts embedded errors. Convenience
// function written to handle all cases where we have known types of
// errors returned by underlying layers.
func ToAPIError(ctx context.Context, err error) APIError {
	if err == nil {
		return noError
	}

	var apiErr = ErrorCodes.ToAPIErr(toAPIErrorCode(ctx, err))

	if apiErr.Code == "InternalError" {
		// If we see an internal storageerror try to interpret
		// any underlying errors if possible depending on
		// their internal storageerror types. This code is only
		// useful with gateway implementations.
		switch e := err.(type) {
		case InvalidArgument:
			apiErr = APIError{
				Code:           "InvalidArgument",
				Description:    e.Error(),
				HTTPStatusCode: ErrorCodes[ErrInvalidRequest].HTTPStatusCode,
			}

			// Add more Gateway SDKs here if any in future.
		default:
			apiErr = APIError{
				Code:           apiErr.Code,
				Description:    fmt.Sprintf("%s: cause(%v)", apiErr.Description, err),
				HTTPStatusCode: apiErr.HTTPStatusCode,
			}
		}
	}

	return apiErr
}

// GenericError - generic object layer storageerror.
type GenericError struct {
	Bucket    string
	Object    string
	VersionID string
	Err       error
}

// InvalidArgument incorrect input argument
type InvalidArgument GenericError

func (e InvalidArgument) Error() string {
	if e.Err != nil {
		return "Invalid arguments provided for " + e.Bucket + "/" + e.Object + ": (" + e.Err.Error() + ")"
	}
	return "Invalid arguments provided for " + e.Bucket + "/" + e.Object
}

// BucketNotFound bucket does not exist.
type NotFound GenericError

func (e NotFound) Error() string {
	return "Bucket not found: " + e.Bucket
}

// BucketNotFound bucket does not exist.
type BucketNotFound GenericError

func (e BucketNotFound) Error() string {
	return "Bucket not found: " + e.Bucket
}

// BucketNameInvalid - bucketname provided is invalid.
type BucketNameInvalid GenericError

// Error returns string an error formatted as the given text.
func (e BucketNameInvalid) Error() string {
	return "Bucket name invalid: " + e.Bucket
}

// BucketAlreadyExists the requested bucket name is not available.
type BucketAlreadyExists GenericError

func (e BucketAlreadyExists) Error() string {
	return "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again."
}

// BucketAlreadyOwnedByYou already owned by you.
type BucketAlreadyOwnedByYou GenericError

func (e BucketAlreadyOwnedByYou) Error() string {
	return "Bucket already owned by you: " + e.Bucket
}

// BucketNotEmpty bucket is not empty.
type BucketNotEmpty GenericError

func (e BucketNotEmpty) Error() string {
	return "Bucket not empty: " + e.Bucket
}

// InvalidVersionID invalid version id
type InvalidVersionID GenericError

func (e InvalidVersionID) Error() string {
	return "Invalid version id: " + e.Bucket + "/" + e.Object + "(" + e.VersionID + ")"
}

// VersionNotFound version does not exist.
type VersionNotFound GenericError

func (e VersionNotFound) Error() string {
	return "Version not found: " + e.Bucket + "/" + e.Object + "(" + e.VersionID + ")"
}

// ObjectNotFound object does not exist.
type ObjectNotFound GenericError

func (e ObjectNotFound) Error() string {
	return "Object not found: " + e.Bucket + "/" + e.Object
}

// MethodNotAllowed on the object
type MethodNotAllowed GenericError

func (e MethodNotAllowed) Error() string {
	return "Method not allowed: " + e.Bucket + "/" + e.Object
}

// FileSystemError on the object
type FileSystemError GenericError

func (e FileSystemError) Error() string {
	return "file system storageerror: " + e.Bucket + "/" + e.Object
}

// IncompleteBody You did not provide the number of bytes specified by the Content-Length HTTP header.
type IncompleteBody GenericError

// Error returns string an storageerror formatted as the given text.
func (e IncompleteBody) Error() string {
	return e.Bucket + "/" + e.Object + "has incomplete body"
}

// InvalidPart One or more of the specified parts could not be found
type InvalidPart struct {
	PartNumber int
	ExpETag    string
	GotETag    string
}

func (e InvalidPart) Error() string {
	return fmt.Sprintf("Specified part could not be found. PartNumber %d, Expected %s, got %s",
		e.PartNumber, e.ExpETag, e.GotETag)
}

// BucketLifecycleNotFound - no bucket lifecycle found.
type BucketLifecycleNotFound GenericError

func (e BucketLifecycleNotFound) Error() string {
	return "No bucket lifecycle configuration found for bucket : " + e.Bucket
}

// BucketTaggingNotFound - no bucket tags found
type BucketTaggingNotFound GenericError

func (e BucketTaggingNotFound) Error() string {
	return "No bucket tags found for bucket: " + e.Bucket
}

// BucketACLNotFound - no bucket acl found.
type BucketACLNotFound GenericError

func (e BucketACLNotFound) Error() string {
	return "No bucket acl configuration found for bucket : " + e.Bucket
}

// BucketLoggingNotFound - no bucket acl found.
type BucketLoggingNotFound GenericError

func (e BucketLoggingNotFound) Error() string {
	return "No bucket logging configuration found for bucket : " + e.Bucket
}

// BucketPolicyNotFound - no bucket policy found.
type BucketPolicyNotFound GenericError

func (e BucketPolicyNotFound) Error() string {
	return "No bucket policy configuration found for bucket: " + e.Bucket
}

// ObjectTaggingNotFound - no object tags found
type ObjectTaggingNotFound GenericError

func (e ObjectTaggingNotFound) Error() string {
	return "No object tags found for object: " + e.Bucket + "," + e.Object
}

// ObjectACLNotFound - no object ACL found
type ObjectACLNotFound GenericError

func (e ObjectACLNotFound) Error() string {
	return "No object ACL found for object: " + e.Bucket + "," + e.Object
}

// WriteDataBaseFailed - write database failed.
type WriteDataBaseFailed GenericError

func (e WriteDataBaseFailed) Error() string {
	return "Write database failed : " + e.Err.Error()
}
