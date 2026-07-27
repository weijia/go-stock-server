// go-stock-server/mqtt_crypto.go - OpenSSL 兼容 AES 加解密（与 Python mqtt_crypto 互通）
package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"errors"
)

var saltMagic = []byte("Salted__")

// CryptoError 加解密失败（密钥不匹配 / 格式错误）
type CryptoError struct{ msg string }

func (e *CryptoError) Error() string { return e.msg }

// evpBytesToKey EVP_BytesToKey（MD5，1 次迭代）派生 key + iv（与 CryptoJS.AES 兼容）
func evpBytesToKey(password, salt []byte, keyLen, ivLen int) ([]byte, []byte) {
	dtot := make([]byte, 0, keyLen+ivLen)
	prev := make([]byte, 0)
	for len(dtot) < keyLen+ivLen {
		h := md5.New()
		h.Write(prev)
		h.Write(password)
		h.Write(salt)
		prev = h.Sum(nil)
		dtot = append(dtot, prev...)
	}
	return dtot[:keyLen], dtot[keyLen : keyLen+ivLen]
}

// encrypt 加密明文 → base64( "Salted__" ‖ salt(8B) ‖ AES-256-CBC(PKCS7(utf8)) )
func encrypt(plaintext, password string) (string, error) {
	salt := make([]byte, 8)
	if _, err := rand.Read(salt); err != nil {
		return "", &CryptoError{"salt 生成失败: " + err.Error()}
	}
	key, iv := evpBytesToKey([]byte(password), salt, 32, 16)
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", &CryptoError{err.Error()}
	}
	padded := pkcs7Pad([]byte(plaintext), aes.BlockSize)
	mode := cipher.NewCBCEncrypter(block, iv)
	ct := make([]byte, len(padded))
	mode.CryptBlocks(ct, padded)

	out := make([]byte, 0, len(saltMagic)+8+len(ct))
	out = append(out, saltMagic...)
	out = append(out, salt...)
	out = append(out, ct...)
	return base64.StdEncoding.EncodeToString(out), nil
}

// decrypt 解密 base64 密文 → 明文
func decrypt(ciphertextB64, password string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return "", &CryptoError{"base64 解码失败: " + err.Error()}
	}
	if len(data) < 16 || string(data[:8]) != string(saltMagic) {
		return "", &CryptoError{"密文不是 OpenSSL Salted__ 格式"}
	}
	salt := data[8:16]
	ct := data[16:]
	if len(ct) == 0 || len(ct)%aes.BlockSize != 0 {
		return "", &CryptoError{"密文长度非法（非 16 字节对齐）"}
	}
	key, iv := evpBytesToKey([]byte(password), salt, 32, 16)
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", &CryptoError{err.Error()}
	}
	mode := cipher.NewCBCDecrypter(block, iv)
	pt := make([]byte, len(ct))
	mode.CryptBlocks(pt, ct)
	unpadded, err := pkcs7Unpad(pt, aes.BlockSize)
	if err != nil {
		return "", &CryptoError{"解密失败（密钥不匹配?）"}
	}
	return string(unpadded), nil
}

func pkcs7Pad(data []byte, blockSize int) []byte {
	pad := blockSize - len(data)%blockSize
	out := make([]byte, len(data)+pad)
	copy(out, data)
	for i := len(data); i < len(out); i++ {
		out[i] = byte(pad)
	}
	return out
}

func pkcs7Unpad(data []byte, blockSize int) ([]byte, error) {
	if len(data) == 0 || len(data)%blockSize != 0 {
		return nil, errors.New("bad padding")
	}
	pad := int(data[len(data)-1])
	if pad <= 0 || pad > blockSize {
		return nil, errors.New("bad padding")
	}
	return data[:len(data)-pad], nil
}
