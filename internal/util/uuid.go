package util

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"time"
)

func UUIDv7() string {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)

	if _, err := rand.Read(b[6:]); err != nil {
		r := mrand.New(mrand.NewSource(time.Now().UnixNano()))
		if _, err2 := r.Read(b[6:]); err2 != nil {
			panic(fmt.Errorf("uuidv7 random read failed: %w", err2))
		}
	}

	b[6] &= 0x0f
	b[6] |= 0x70
	b[8] &= 0x3f
	b[8] |= 0x80

	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[0], b[1], b[2], b[3],
		b[4], b[5],
		b[6], b[7],
		b[8], b[9],
		b[10], b[11], b[12], b[13], b[14], b[15],
	)
}
