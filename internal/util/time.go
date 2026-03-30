package util

import "time"

var fallbackKST = time.FixedZone("Asia/Seoul", 9*60*60)

func KST() *time.Location {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		return fallbackKST
	}
	return loc
}

func NowKST() time.Time {
	return time.Now().In(KST())
}

func FormatCHDateTime64Millis(t time.Time) string {
	return t.In(KST()).Format("2006-01-02 15:04:05.000")
}

func FormatCHDateTimeSeconds(t time.Time) string {
	return t.In(KST()).Format("2006-01-02 15:04:05")
}
