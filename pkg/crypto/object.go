package crypto

import (
	"io"

	"github.com/minio/sio"
	"mtcloud.com/mtstorage/pkg/etag"
	"mtcloud.com/mtstorage/pkg/fips"
	"mtcloud.com/mtstorage/pkg/hash"
)

func GetEncryptReader(read io.Reader, objectEncryptionKey hash.ObjectKey, encMd5Sum string, size int64) (*hash.Reader, error) {
	reader, err := sio.EncryptReader(read, sio.Config{Key: objectEncryptionKey[:], MinVersion: sio.Version20, CipherSuites: fips.CipherSuitesDARE()})
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	hashReader, err := hash.NewReader(etag.Wrap(reader, read), size, encMd5Sum, "", size)
	if err != nil {
		return nil, err
	}
	// rclone 使用分段上传的时候上传完成后校验的MD5 不能经过加密
	hashReader.WithEncryption(objectEncryptionKey)
	return hashReader, nil
}
