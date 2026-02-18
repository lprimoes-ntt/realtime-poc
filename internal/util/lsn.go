package util

import (
	"bytes"
	"encoding/hex"
)

var ZeroLSN = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

func IsZeroLSN(lsn []byte) bool {
	if len(lsn) == 0 {
		return true
	}
	return bytes.Equal(PadLSN(lsn), ZeroLSN)
}

func PadLSN(lsn []byte) []byte {
	if len(lsn) >= 10 {
		if len(lsn) == 10 {
			return append([]byte(nil), lsn...)
		}
		return append([]byte(nil), lsn[len(lsn)-10:]...)
	}
	padded := make([]byte, 10)
	copy(padded[10-len(lsn):], lsn)
	return padded
}

func CompareLSN(a, b []byte) int {
	return bytes.Compare(PadLSN(a), PadLSN(b))
}

func LSNHex(lsn []byte) string {
	if len(lsn) == 0 {
		return "0x"
	}
	return "0x" + hex.EncodeToString(lsn)
}
