package hash

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"strconv"
	"strings"

	"github.com/minio/sio"
	"mtcloud.com/mtstorage/pkg/etag"
	"mtcloud.com/mtstorage/pkg/fips"
	"mtcloud.com/mtstorage/pkg/logger"
)

var errObjectTampered = errors.New("The requested object was modified and may be compromised")

type ObjectKey [32]byte

type SealMD5CurrFn func([]byte) []byte

// A Reader wraps an io.Reader and computes the MD5 checksum
// of the read content as ETag. Optionally, it also computes
// the SHA256 checksum of the content.
//
// If the reference values for the ETag and content SHA26
// are not empty then it will check whether the computed
// match the reference values.
type Reader struct {
	src       io.Reader
	bytesRead int64

	size       int64
	actualSize int64

	checksum      etag.ETag
	contentSHA256 []byte

	sha256    hash.Hash
	sealMD5Fn SealMD5CurrFn
}

// NewReader returns a new Reader that wraps src and computes
// MD5 checksum of everything it reads as ETag.
//
// It also computes the SHA256 checksum of everything it reads
// if sha256Hex is not the empty string.
//
// If size resp. actualSize is unknown at the time of calling
// NewReader then it should be set to -1.
//
// NewReader may try merge the given size, MD5 and SHA256 values
// into src - if src is a Reader - to avoid computing the same
// checksums multiple times.
func NewReader(src io.Reader, size int64, md5Hex, sha256Hex string, actualSize int64) (*Reader, error) {
	MD5, err := hex.DecodeString(md5Hex)
	if err != nil {
		return nil, BadDigest{ // TODO(aead): Return an error that indicates that an invalid ETag has been specified
			ExpectedMD5:   md5Hex,
			CalculatedMD5: "",
		}
	}
	SHA256, err := hex.DecodeString(sha256Hex)
	if err != nil {
		return nil, SHA256Mismatch{ // TODO(aead): Return an error that indicates that an invalid Content-SHA256 has been specified
			ExpectedSHA256:   sha256Hex,
			CalculatedSHA256: "",
		}
	}

	// Merge the size, MD5 and SHA256 values if src is a Reader.
	// The size may be set to -1 by callers if unknown.
	if r, ok := src.(*Reader); ok {
		if r.bytesRead > 0 {
			return nil, errors.New("hash: already read from hash reader")
		}
		if len(r.checksum) != 0 && len(MD5) != 0 && !etag.Equal(r.checksum, etag.ETag(MD5)) {
			return nil, BadDigest{
				ExpectedMD5:   r.checksum.String(),
				CalculatedMD5: md5Hex,
			}
		}
		if len(r.contentSHA256) != 0 && len(SHA256) != 0 && !bytes.Equal(r.contentSHA256, SHA256) {
			return nil, SHA256Mismatch{
				ExpectedSHA256:   hex.EncodeToString(r.contentSHA256),
				CalculatedSHA256: sha256Hex,
			}
		}
		if r.size >= 0 && size >= 0 && r.size != size {
			return nil, ErrSizeMismatch{Want: r.size, Got: size}
		}

		r.checksum = etag.ETag(MD5)
		r.contentSHA256 = SHA256
		if r.size < 0 && size >= 0 {
			r.src = etag.Wrap(io.LimitReader(r.src, size), r.src)
			r.size = size
		}
		if r.actualSize <= 0 && actualSize >= 0 {
			r.actualSize = actualSize
		}
		return r, nil
	}

	if size >= 0 {
		r := io.LimitReader(src, size)
		if _, ok := src.(etag.Tagger); !ok {
			src = etag.NewReader(r, etag.ETag(MD5))
		} else {
			src = etag.Wrap(r, src)
		}
	} else if _, ok := src.(etag.Tagger); !ok {
		src = etag.NewReader(src, etag.ETag(MD5))
	}
	var hash hash.Hash
	if len(SHA256) != 0 {
		hash = newSHA256()
	}
	return &Reader{
		src:           src,
		size:          size,
		actualSize:    actualSize,
		checksum:      etag.ETag(MD5),
		contentSHA256: SHA256,
		sha256:        hash,
	}, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	r.bytesRead += int64(n)
	if r.sha256 != nil {
		r.sha256.Write(p[:n])
	}

	if err == io.EOF { // Verify content SHA256, if set.
		if r.sha256 != nil {
			if sum := r.sha256.Sum(nil); !bytes.Equal(r.contentSHA256, sum) {
				return n, SHA256Mismatch{
					ExpectedSHA256:   hex.EncodeToString(r.contentSHA256),
					CalculatedSHA256: hex.EncodeToString(sum),
				}
			}
		}
	}
	if err != nil && err != io.EOF {
		if v, ok := err.(etag.VerifyError); ok {
			return n, BadDigest{
				ExpectedMD5:   v.Expected.String(),
				CalculatedMD5: v.Computed.String(),
			}
		}
	}
	return n, err
}

// Size returns the absolute number of bytes the Reader
// will return during reading. It returns -1 for unlimited
// data.
func (r *Reader) Size() int64 { return r.size }

// ActualSize returns the pre-modified size of the object.
// DecompressedSize - For compressed objects.
func (r *Reader) ActualSize() int64 { return r.actualSize }

// ETag returns the ETag computed by an underlying etag.Tagger.
// If the underlying io.Reader does not implement etag.Tagger
// it returns nil.
func (r *Reader) ETag() etag.ETag {
	if t, ok := r.src.(etag.Tagger); ok {
		return t.ETag()
	}
	return nil
}

// MD5 returns the MD5 checksum set as reference value.
//
// It corresponds to the checksum that is expected and
// not the actual MD5 checksum of the content.
// Therefore, refer to MD5Current.
func (r *Reader) MD5() []byte {
	return r.checksum
}

// MD5Current returns the MD5 checksum of the content
// that has been read so far.
//
// Calling MD5Current again after reading more data may
// result in a different checksum.
func (r *Reader) MD5Current() []byte {
	return r.ETag()[:]
}

// SHA256 returns the SHA256 checksum set as reference value.
//
// It corresponds to the checksum that is expected and
// not the actual SHA256 checksum of the content.
func (r *Reader) SHA256() []byte {
	return r.contentSHA256
}

// MD5HexString returns a hex representation of the MD5.
func (r *Reader) MD5HexString() string {
	return hex.EncodeToString(r.checksum)
}

// MD5Base64String returns a hex representation of the MD5.
func (r *Reader) MD5Base64String() string {
	return base64.StdEncoding.EncodeToString(r.checksum)
}

// SHA256HexString returns a hex representation of the SHA256.
func (r *Reader) SHA256HexString() string {
	return hex.EncodeToString(r.contentSHA256)
}

var _ io.Closer = (*Reader)(nil) // compiler check

// Close and release resources.
func (r *Reader) Close() error { return nil }

// BytesRead 返回读取的字节数
func (r *Reader) BytesRead() int64 {
	return r.bytesRead
}

// WithEncryption sets up encrypted reader and the sealing for content md5sum
// using objEncKey. Unsealed md5sum is computed from the rawReader setup when
// NewPutObjReader was called. It returns an error if called on an uninitialized
// PutObjReader.
func (p *Reader) WithEncryption(objEncKey ObjectKey) {
	p.sealMD5Fn = sealETagFn(objEncKey)
}

func sealETagFn(key ObjectKey) SealMD5CurrFn {
	fn := func(md5sumcurr []byte) []byte {
		return sealETag(key, md5sumcurr)
	}
	return fn
}

func sealETag(encKey ObjectKey, md5CurrSum []byte) []byte {
	var emptyKey [32]byte
	if bytes.Equal(encKey[:], emptyKey[:]) {
		return md5CurrSum
	}
	return encKey.SealETag(md5CurrSum)
}

// DecryptETag decrypts the ETag that is part of given object
// with the given object encryption key.
//
// However, DecryptETag does not try to decrypt the ETag if
// it consists of a 128 bit hex value (32 hex chars) and exactly
// one '-' followed by a 32-bit number.
// This special case adresses randomly-generated ETags generated
// by the MinIO server when running in non-compat mode. These
// random ETags are not encrypt.
//
// Calling DecryptETag with a non-randomly generated ETag will
// fail.
func DecryptETag(key ObjectKey, ETag string) (string, error) {
	if n := strings.Count(ETag, "-"); n > 0 {
		if n != 1 {
			return "", errObjectTampered
		}
		i := strings.IndexByte(ETag, '-')
		if len(ETag[:i]) != 32 {
			return "", errObjectTampered
		}
		if _, err := hex.DecodeString(ETag[:32]); err != nil {
			return "", errObjectTampered
		}
		if _, err := strconv.ParseInt(ETag[i+1:], 10, 32); err != nil {
			return "", errObjectTampered
		}
		return ETag, nil
	}

	etag, err := hex.DecodeString(ETag)
	if err != nil {
		return "", err
	}
	etag, err = key.UnsealETag(etag)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(etag), nil
}

// SealETag seals the etag using the object key.
// It does not encrypt empty ETags because such ETags indicate
// that the S3 client hasn't sent an ETag = MD5(object) and
// the backend can pick an ETag value.
func (key ObjectKey) SealETag(etag []byte) []byte {
	if len(etag) == 0 { // don't encrypt empty ETag - only if client sent ETag = MD5(object)
		return etag
	}
	var buffer bytes.Buffer
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte("SSE-etag"))
	if _, err := sio.Encrypt(&buffer, bytes.NewReader(etag), sio.Config{Key: mac.Sum(nil), CipherSuites: fips.CipherSuitesDARE()}); err != nil {
		logger.Warn(context.Background(), errors.New("Unable to encrypt ETag using object key"))
	}
	return buffer.Bytes()
}

func (key ObjectKey) UnsealETag(etag []byte) ([]byte, error) {
	if !IsETagSealed(etag) {
		return etag, nil
	}
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte("SSE-etag"))
	return sio.DecryptBuffer(make([]byte, 0, len(etag)), etag, sio.Config{Key: mac.Sum(nil), CipherSuites: fips.CipherSuitesDARE()})
}

func IsETagSealed(etag []byte) bool { return len(etag) > 16 }
