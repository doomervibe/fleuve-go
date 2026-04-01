package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
)

// Errors returned by crypto operations
var (
	ErrInvalidKeySize     = errors.New("crypto: invalid key size")
	ErrCiphertextTooShort = errors.New("crypto: ciphertext too short")
	ErrInvalidPadding     = errors.New("crypto: invalid padding")
	ErrInvalidBlockSize   = errors.New("crypto: data is not block-aligned")
)

// blockSize is the AES block size (16 bytes)
const blockSize = aes.BlockSize

// DeriveKey derives a 32-byte AES-256 key from arbitrary input using SHA256.
// This should be used to derive the encryption key from STORAGE_KEY env var.
func DeriveKey(key []byte) []byte {
	hash := sha256.Sum256(key)
	return hash[:]
}

// pkcs7Pad pads data to a multiple of blockSize using PKCS7 padding scheme.
func pkcs7Pad(data []byte, blockSize int) []byte {
	padding := blockSize - (len(data) % blockSize)
	padText := make([]byte, padding)
	for i := range padText {
		padText[i] = byte(padding)
	}
	return append(data, padText...)
}

// pkcs7Unpad removes PKCS7 padding from data.
func pkcs7Unpad(data []byte, blockSize int) ([]byte, error) {
	if len(data) == 0 {
		return nil, ErrInvalidPadding
	}
	if len(data)%blockSize != 0 {
		return nil, ErrInvalidBlockSize
	}

	// Get padding value from last byte
	padding := int(data[len(data)-1])

	// Validate padding value
	if padding == 0 || padding > blockSize || padding > len(data) {
		return nil, ErrInvalidPadding
	}

	// Verify all padding bytes match
	for i := len(data) - padding; i < len(data); i++ {
		if data[i] != byte(padding) {
			return nil, ErrInvalidPadding
		}
	}

	return data[:len(data)-padding], nil
}

// Encrypt encrypts plaintext using AES-256-CBC with PKCS7 padding.
// The key must be exactly 32 bytes (use DeriveKey to derive from raw key material).
// Returns ciphertext in format: nonce(16) + AES-CBC(padded_plaintext).
func Encrypt(plaintext []byte, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKeySize
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// Apply PKCS7 padding
	paddedPlaintext := pkcs7Pad(plaintext, blockSize)

	// Generate random 16-byte nonce (IV)
	nonce := make([]byte, blockSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Encrypt using CBC mode
	ciphertext := make([]byte, len(paddedPlaintext))
	mode := cipher.NewCBCEncrypter(block, nonce)
	mode.CryptBlocks(ciphertext, paddedPlaintext)

	// Prepend nonce to ciphertext: nonce(16) + ciphertext
	result := make([]byte, blockSize+len(ciphertext))
	copy(result[:blockSize], nonce)
	copy(result[blockSize:], ciphertext)

	return result, nil
}

// Decrypt decrypts ciphertext that was encrypted with Encrypt.
// Expects ciphertext in format: nonce(16) + AES-CBC(padded_plaintext).
// The key must be exactly 32 bytes.
func Decrypt(ciphertext []byte, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKeySize
	}
	if len(ciphertext) < blockSize {
		return nil, ErrCiphertextTooShort
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// Extract nonce (first 16 bytes) and encrypted data
	nonce := ciphertext[:blockSize]
	encryptedData := ciphertext[blockSize:]

	// Validate that encrypted data length is a multiple of block size
	if len(encryptedData) == 0 || len(encryptedData)%blockSize != 0 {
		return nil, ErrInvalidBlockSize
	}

	// Decrypt using CBC mode
	plaintext := make([]byte, len(encryptedData))
	mode := cipher.NewCBCDecrypter(block, nonce)
	mode.CryptBlocks(plaintext, encryptedData)

	// Remove PKCS7 padding
	return pkcs7Unpad(plaintext, blockSize)
}
