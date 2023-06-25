package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/hex"
)

type AesHandler struct {
	Key       []byte
	BlockSize int
}

func NewAesHnadler(key []byte, blockSize int) *AesHandler {
	return &AesHandler{Key: key, BlockSize: blockSize}
}

func (h *AesHandler) padding(src []byte) []byte {
	paddingCount := aes.BlockSize - len(src)%aes.BlockSize
	if paddingCount == 0 {
		return src
	} else {
		return append(src, bytes.Repeat([]byte{byte(0)}, paddingCount)...)
	}
}

func (h *AesHandler) unPadding(src []byte) []byte {
	for i := len(src) - 1; ; i-- {
		if src[i] != 0 {
			return src[:i+1]
		}
	}
}

func (h *AesHandler) Encrypt(src []byte) ([]byte, error) {
	block, err := aes.NewCipher([]byte(h.Key))
	if err != nil {
		return nil, err
	}

	src = h.padding(src)
	encryptData := make([]byte, len(src))
	tmpBlock := make([]byte, h.BlockSize)

	for i := 0; i < len(src); i += h.BlockSize {
		block.Encrypt(tmpBlock, src[i:i+h.BlockSize])
		copy(encryptData[i:i+h.BlockSize], tmpBlock)
	}
	return encryptData, nil
}

func (h *AesHandler) Decrypt(src []byte) ([]byte, error) {
	block, err := aes.NewCipher([]byte(h.Key))
	if err != nil {
		return nil, err
	}
	decryptData := make([]byte, len(src))
	tmpBlock := make([]byte, h.BlockSize)

	for i := 0; i < len(src); i += h.BlockSize {
		block.Decrypt(tmpBlock, src[i:i+h.BlockSize])
		copy(decryptData[i:i+h.BlockSize], tmpBlock)
	}
	return h.unPadding(decryptData), nil
}

func Base64Encrypt(key, cid string) (string, error) {
	sDec, err := base64.URLEncoding.DecodeString(key)
	if err != nil {
		return "", err
	}
	a := NewAesHnadler(sDec, 16)
	data, err := a.Encrypt([]byte(cid))
	return base64.URLEncoding.EncodeToString(data), err
}

func Base64Decrypt(key, cid string) (string, error) {
	sDec, err := base64.URLEncoding.DecodeString(key)
	if err != nil {
		return "", err
	}
	cidDe, err := base64.URLEncoding.DecodeString(cid)
	//data, err := hex.DecodeString(cid)
	if err != nil {
		return "", err
	}
	a := NewAesHnadler(sDec, 16)
	res, err := a.Decrypt(cidDe)
	//index := bytes.IndexByte(res, []byte("\u0002")[0])
	//res = res[:index]
	return string(res), err
}

const salt = "mtyw-oss-password-12345678901234"

// @brief:填充明文
func pKCS5Padding(plaintext []byte, blockSize int) []byte {
	padding := blockSize - len(plaintext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(plaintext, padtext...)
}

// @brief:去除填充数据
func pKCS5UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

// @brief:AES加密
func aesEncrypt(origData, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	//AES分组长度为128位，所以blockSize=16，单位字节
	blockSize := block.BlockSize()
	origData = pKCS5Padding(origData, blockSize)
	blockMode := cipher.NewCBCEncrypter(block, key[:blockSize]) //初始向量的长度必须等于块block的长度16字节
	crypted := make([]byte, len(origData))
	blockMode.CryptBlocks(crypted, origData)
	return crypted, nil
}

// @brief:AES解密
func aesDecrypt(crypted, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	//AES分组长度为128位，所以blockSize=16，单位字节
	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize]) //初始向量的长度必须等于块block的长度16字节
	origData := make([]byte, len(crypted))
	blockMode.CryptBlocks(origData, crypted)
	origData = pKCS5UnPadding(origData)
	return origData, nil
}

func EncryptLocalPassword(password string) string {
	encrypt, err := aesEncrypt([]byte(password), []byte(salt))
	if err != nil {
		return ""
	}
	return hex.EncodeToString(encrypt)
}

func DecryptLocalPassword(cipher string) string {
	decodeString, err := hex.DecodeString(cipher)
	if err != nil {
		return ""
	}
	decrypt, err := aesDecrypt(decodeString, []byte(salt))
	if err != nil {
		return ""
	}
	return string(decrypt)
}
