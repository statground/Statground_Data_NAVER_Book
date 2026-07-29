package workflowcontract

import (
	"os"
	"strings"
	"testing"
)

func TestBookProviderSchedulesShareNonCancellingConcurrency(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"../../.github/workflows/naver_book_schedule.yml",
		"../../.github/workflows/kakao_book_schedule.yml",
	} {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(source)
		if !strings.Contains(text, "group: statground-book-provider-schedule") {
			t.Errorf("%s does not use the shared provider schedule group", path)
		}
		if !strings.Contains(text, "cancel-in-progress: false") {
			t.Errorf("%s can still cancel an in-progress provider collection", path)
		}
	}
}
