package hash

import (
	"encoding/hex"
	"math/rand"
	"regexp"
	"strings"

	"mtcloud.com/mtstorage/util"
)

var (
	etagRegex = regexp.MustCompile("\"*?([^\"]*?)\"*?$")
)

// GenETag - generate UUID based ETag
func GenETag() string {
	return ToS3ETag(getMD5Hash([]byte(util.MustGetUUID())))
}

// ToS3ETag - return checksum to ETag
func ToS3ETag(etag string) string {
	etag = canonicalizeETag(etag)

	if !strings.HasSuffix(etag, "-1") {
		// Tools like s3cmd uses ETag as checksum of data to validate.
		// Append "-1" to indicate ETag is not a checksum.
		etag += "-1"
	}

	return etag
}

// canonicalizeETag returns ETag with leading and trailing double-quotes removed,
// if any present
func canonicalizeETag(etag string) string {
	return etagRegex.ReplaceAllString(etag, "$1")
}

// MD5CurrentHexString returns the current MD5Sum or encrypted MD5Sum
// as a hex encoded string
func (r *Reader) MD5CurrentHexString() string {
	md5sumCurr := r.MD5Current()
	var appendHyphen bool
	// md5sumcurr is not empty in two scenarios
	// - server is running in strict compatibility mode
	// - client set Content-Md5 during PUT operation
	if len(md5sumCurr) == 0 {
		// md5sumCurr is only empty when we are running
		// in non-compatibility mode.
		md5sumCurr = make([]byte, 16)
		rand.Read(md5sumCurr)
		appendHyphen = true
	}
	if r.sealMD5Fn != nil {
		md5sumCurr = r.sealMD5Fn(md5sumCurr)
	}
	if appendHyphen {
		// Make sure to return etag string upto 32 length, for SSE
		// requests ETag might be longer and the code decrypting the
		// ETag ignores ETag in multipart ETag form i.e <hex>-N
		return hex.EncodeToString(md5sumCurr)[:32] + "-1"
	}
	return hex.EncodeToString(md5sumCurr)
}
