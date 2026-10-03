package bookrefreshreceipt

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestReceiptPreservesActualWatermarkAndRejectsUnsafeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	want := Receipt{Version: 1, RunUUID: "run", State: "completed", StartedMillis: 2000000000000, Inserted: 5, LatestInsertedMillis: 2000000000123, CompletedMillis: 2000000000500}
	if err := Write(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := Read(path)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(path); err == nil {
		t.Fatal("publicly readable collection receipt accepted")
	}
	link := filepath.Join(t.TempDir(), "link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(link); err == nil {
		t.Fatal("symlink collection receipt accepted")
	}
}
func TestReceiptRequiresWatermarkAfterSuccessfulInsert(t *testing.T) {
	if err := (Receipt{Version: 1, RunUUID: "run", State: "completed", StartedMillis: 500, Inserted: 1, CompletedMillis: 1000}).Validate(); err == nil {
		t.Fatal("successful insert without its actual watermark accepted")
	}
	if err := (Receipt{Version: 1, RunUUID: "run", State: "completed", StartedMillis: 500, Inserted: 0, CompletedMillis: 1000}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestPrecollectionReceiptCannotAuthorizeNativeRefresh(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	pending := Receipt{Version: 1, RunUUID: "run", State: "collecting", StartedMillis: 500}
	if err := Write(path, pending); err != nil {
		t.Fatal(err)
	}
	got, err := Read(path)
	if err != nil || got.ValidateCompleted() == nil {
		t.Fatalf("unconfirmed receipt accepted for native refresh: %+v err=%v", got, err)
	}
}
