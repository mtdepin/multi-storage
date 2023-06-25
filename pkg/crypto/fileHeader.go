package crypto

import (
	"errors"
	"io"

	"github.com/minio/sio"
)

// mtyw-oss-header  sha1转换而来
const header = "e6edc421c0c7c6bf326ccd5e010fb115c217fe4a"
const Size = 1024 * 1024 * 2

// AddHeader 文件添加标志头
func AddHeader(info []byte) [Size]byte {
	var content [Size]byte
	h := []byte(header)
	hLen := len(h)
	copy(content[:hLen], h)       // 添加头
	copy(content[hLen:], info[:]) // 添加信息
	copy(content[Size-hLen:], h)  // 添加头
	return content
}

// CheckHeader 检验是否是加密上传
func CheckHeader(b [Size]byte) ([]byte, bool) {
	r := string(b[:40])
	return b[40 : Size-40], r == header && string(b[Size-40:]) == header
}
func NewReader(src io.Reader, fileInfo []byte) (io.Reader, error) {
	if len(fileInfo) >= Size-len(header)*2 {
		return nil, errors.New("内容太大")
	}
	headInfo := AddHeader(fileInfo)
	//copy(fileInfo, headInfo[:])
	pr, pw := io.Pipe()
	go func() {
		for i := 0; i < len(headInfo[:]); {
			write, _ := pw.Write(headInfo[:])
			//fmt.Println("-----------...", write)
			i += write
		}
		_, err := io.Copy(pw, src)
		pw.CloseWithError(err)
	}()
	return pr, nil
}

func OrSize(size int64) (uint64, error) {
	return sio.DecryptedSize(uint64(size))
}
