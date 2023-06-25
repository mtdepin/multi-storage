package api

import (
	"context"
	"encoding/json"
	"encoding/xml"
	xhttp "mtcloud.com/mtstorage/pkg/http"
	"mtcloud.com/mtstorage/pkg/logger"
	error2 "mtcloud.com/mtstorage/pkg/storageerror"
	"net/http"
	"net/url"
	"strconv"
)

const (
	// RFC3339 a subset of the ISO8601 timestamp format. e.g 2014-04-29T18:30:38Z
	iso8601TimeFormat = "2006-01-02T15:04:05.000Z" // Reply date format with nanosecond precision.
	maxObjectList     = 1000                       // Limit number of objects in a listObjectsResponse/listObjectsVersionsResponse.
	maxDeleteList     = 10000                      // Limit number of objects deleted in a delete call.
	maxUploadsList    = 10000                      // Limit number of uploads in a listUploadsResponse.
	maxPartsList      = 10000                      // Limit number of parts in a listPartsResponse.
)

// LocationResponse - format for location response.
type LocationResponse struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint" json:"-"`
	Location string   `xml:",chardata"`
}

// PolicyStatus captures information returned by GetBucketPolicyStatusHandler
type PolicyStatus struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ PolicyStatus" json:"-"`
	IsPublic string
}

// ListVersionsResponse - format for list bucket versions response.
type ListVersionsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListVersionsResult" json:"-"`

	Name      string
	Prefix    string
	KeyMarker string

	// When response is truncated (the IsTruncated element value in the response
	// is true), you can use the key name in this field as marker in the subsequent
	// request to get next set of objects. Server lists objects in alphabetical
	// order Note: This element is returned only if you have delimiter request parameter
	// specified. If response does not include the NextMaker and it is truncated,
	// you can use the value of the last Key in the response as the marker in the
	// subsequent request to get the next set of object keys.
	NextKeyMarker string `xml:"NextKeyMarker,omitempty"`

	// When the number of responses exceeds the value of MaxKeys,
	// NextVersionIdMarker specifies the first object version not
	// returned that satisfies the search criteria. Use this value
	// for the version-id-marker request parameter in a subsequent request.
	NextVersionIDMarker string `xml:"NextVersionIdMarker"`

	// Marks the last version of the Key returned in a truncated response.
	VersionIDMarker string `xml:"VersionIdMarker"`

	MaxKeys   int
	Delimiter string
	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated bool

	CommonPrefixes []CommonPrefix
	Versions       []ObjectVersion

	// Encoding type used to encode object keys in the response.
	EncodingType string `xml:"EncodingType,omitempty"`
}

// ListObjectsResponse - format for list objects response.
type ListObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult" json:"-"`

	Name   string
	Prefix string
	Marker string

	// When response is truncated (the IsTruncated element value in the response
	// is true), you can use the key name in this field as marker in the subsequent
	// request to get next set of objects. Server lists objects in alphabetical
	// order Note: This element is returned only if you have delimiter request parameter
	// specified. If response does not include the NextMaker and it is truncated,
	// you can use the value of the last Key in the response as the marker in the
	// subsequent request to get the next set of object keys.
	NextMarker string `xml:"NextMarker,omitempty"`

	MaxKeys   int
	Delimiter string
	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated bool

	Contents       []Object
	CommonPrefixes []CommonPrefix

	// Encoding type used to encode object keys in the response.
	EncodingType string `xml:"EncodingType,omitempty"`
}

// ListObjectsV2Response - format for list objects response.
type ListObjectsV2Response struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult" json:"-"`

	Name       string
	Prefix     string
	StartAfter string `xml:"StartAfter,omitempty"`
	// When response is truncated (the IsTruncated element value in the response
	// is true), you can use the key name in this field as marker in the subsequent
	// request to get next set of objects. Server lists objects in alphabetical
	// order Note: This element is returned only if you have delimiter request parameter
	// specified. If response does not include the NextMaker and it is truncated,
	// you can use the value of the last Key in the response as the marker in the
	// subsequent request to get the next set of object keys.
	ContinuationToken     string `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string `xml:"NextContinuationToken,omitempty"`

	KeyCount  int
	MaxKeys   int
	Delimiter string
	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated bool

	Contents       []Object
	CommonPrefixes []CommonPrefix

	// Encoding type used to encode object keys in the response.
	EncodingType string `xml:"EncodingType,omitempty"`
}

// Part container for part metadata.
type Part struct {
	PartNumber   int
	LastModified string
	ETag         string
	Size         int64
}

// ListPartsResponse - format for list parts response.
type ListPartsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListPartsResult" json:"-"`

	Bucket   string
	Key      string
	UploadID string `xml:"UploadId"`

	Initiator Initiator
	Owner     Owner

	// The class of storage used to store the object.
	StorageClass string

	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int
	IsTruncated          bool

	// List of parts.
	Parts []Part `xml:"Part"`
}

// ListMultipartUploadsResponse - format for list multipart uploads response.
type ListMultipartUploadsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListMultipartUploadsResult" json:"-"`

	Bucket             string
	KeyMarker          string
	UploadIDMarker     string `xml:"UploadIdMarker"`
	NextKeyMarker      string
	NextUploadIDMarker string `xml:"NextUploadIdMarker"`
	Delimiter          string
	Prefix             string
	EncodingType       string `xml:"EncodingType,omitempty"`
	MaxUploads         int
	IsTruncated        bool

	// List of pending uploads.
	Uploads []Upload `xml:"Upload"`

	// Delimed common prefixes.
	CommonPrefixes []CommonPrefix
}

// ListBucketsResponse - format for list buckets response
type ListBucketsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult" json:"-"`

	Owner Owner

	// Container for one or more buckets.
	Buckets struct {
		Buckets []Bucket `xml:"Bucket"`
	} // Buckets are nested
}

// Upload container for in progress multipart upload
type Upload struct {
	Key          string
	UploadID     string `xml:"UploadId"`
	Initiator    Initiator
	Owner        Owner
	StorageClass string
	Initiated    string
}

// CommonPrefix container for prefix response in ListObjectsResponse
type CommonPrefix struct {
	Prefix string
}

// Bucket container for bucket metadata
type Bucket struct {
	Name         string
	CreationDate string // time string of format "2006-01-02T15:04:05.000Z"
}

// ObjectVersion container for object version metadata
type ObjectVersion struct {
	Object
	IsLatest  bool
	VersionID string `xml:"VersionId"`

	isDeleteMarker bool
}

// MarshalXML - marshal ObjectVersion
func (o ObjectVersion) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if o.isDeleteMarker {
		start.Name.Local = "DeleteMarker"
	} else {
		start.Name.Local = "Version"
	}

	type objectVersionWrapper ObjectVersion
	return e.EncodeElement(objectVersionWrapper(o), start)
}

// StringMap is a map[string]string.
type StringMap map[string]string

// MarshalXML - StringMap marshals into XML.
func (s StringMap) MarshalXML(e *xml.Encoder, start xml.StartElement) error {

	tokens := []xml.Token{start}

	for key, value := range s {
		t := xml.StartElement{}
		t.Name = xml.Name{
			Space: "",
			Local: key,
		}
		tokens = append(tokens, t, xml.CharData(value), xml.EndElement{Name: t.Name})
	}

	tokens = append(tokens, xml.EndElement{
		Name: start.Name,
	})

	for _, t := range tokens {
		if err := e.EncodeToken(t); err != nil {
			return err
		}
	}

	// flush to ensure tokens are written
	return e.Flush()
}

// Object container for object metadata
type Object struct {
	Key          string
	LastModified string // time string of format "2006-01-02T15:04:05.000Z"
	ETag         string
	Size         int64

	// Owner of the object.
	Owner Owner

	// The class of storage used to store the object.
	StorageClass string

	// UserMetadata user-defined metadata
	UserMetadata StringMap `xml:"UserMetadata,omitempty"`
}

// Initiator inherit from Owner struct, fields are same
type Initiator Owner

// Owner - bucket owner/principal
type Owner struct {
	ID          string
	DisplayName string
}

// CompleteMultipartUploadResponse container for completed multipart upload response
type CompleteMultipartUploadResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult" json:"-"`

	Location string
	Bucket   string
	Key      string
	ETag     string
}

// DeleteError structure.
type DeleteError struct {
	Code      string
	Message   string
	Key       string
	VersionID string `xml:"VersionId"`
}

func WriteSuccessResponseObject(w http.ResponseWriter, response interface{}) {
	jsonBytes, err := json.Marshal(response)
	if err != nil {
		logger.Errorf("WriteSuccessResponseObject storageerror: ", err)
		return
	}
	writeResponse(w, http.StatusOK, jsonBytes, mimeJSON)
}

// WriteSuccessResponseJSON writes success headers and response if any,
// with content-type set to `application/json`.
func WriteSuccessResponseJSON(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, mimeJSON)
}

// WriteSuccessResponseXML writes success headers and response if any,
// with content-type set to `application/xml`.
func WriteSuccessResponseXML(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, mimeXML)
}

// WriteSuccessNoContent writes success headers with http status 204
func WriteSuccessNoContent(w http.ResponseWriter) {
	writeResponse(w, http.StatusNoContent, nil, mimeNone)
}

func WriteErrorResponseString(ctx context.Context, w http.ResponseWriter, err error2.APIError, reqURL *url.URL) {
	// Generate string storageerror response.
	writeResponse(w, err.HTTPStatusCode, []byte(err.Description), mimeNone)
}

// WriteErrorResponseJSON - writes storageerror response in JSON format;
// useful for admin APIs.
func WriteErrorResponseJSON(w http.ResponseWriter, err error2.APIError, reqURL *url.URL) {
	// Generate storageerror response.
	encodedErrorResponse := encodeResponseJSON(err)
	writeResponse(w, err.HTTPStatusCode, encodedErrorResponse, mimeJSON)
}

// mimeType represents various MIME type used API responses.
type mimeType string

const (
	// Means no response type.
	// Means no response type.
	mimeNone mimeType = ""
	// Means response type is JSON.
	mimeJSON mimeType = "application/json"
	// Means response type is XML.
	mimeXML mimeType = "application/xml"
)

func writeResponse(w http.ResponseWriter, statusCode int, response []byte, mType mimeType) {
	setCommonHeaders(w)
	if mType != mimeNone {
		w.Header().Set(xhttp.ContentType, string(mType))
	}
	w.Header().Set(xhttp.ContentLength, strconv.Itoa(len(response)))
	w.WriteHeader(statusCode)
	if response != nil {
		w.Write(response)
		w.(http.Flusher).Flush()
	}
}
