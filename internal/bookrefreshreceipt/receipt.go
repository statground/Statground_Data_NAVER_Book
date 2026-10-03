package bookrefreshreceipt

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

type Receipt struct {
	Version              int    `json:"version"`
	RunUUID              string `json:"run_uuid"`
	State                string `json:"state"`
	StartedMillis        int64  `json:"started_millis"`
	Inserted             int    `json:"inserted"`
	LatestInsertedMillis int64  `json:"latest_inserted_millis"`
	CompletedMillis      int64  `json:"completed_millis"`
}

func (r Receipt) Validate() error {
	if r.Version != 1 || r.RunUUID == "" || r.StartedMillis <= 0 || r.Inserted < 0 || r.LatestInsertedMillis < 0 {
		return errors.New("book collection receipt invalid")
	}
	if r.State == "collecting" && r.Inserted == 0 && r.LatestInsertedMillis == 0 && r.CompletedMillis == 0 {
		return nil
	}
	if r.State != "completed" || r.CompletedMillis < r.StartedMillis || r.LatestInsertedMillis > r.CompletedMillis || r.Inserted > 0 && r.LatestInsertedMillis == 0 {
		return errors.New("book collection receipt invalid")
	}
	return nil
}

func (r Receipt) ValidateCompleted() error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.State != "completed" {
		return errors.New("book collection receipt has no source confirmation")
	}
	return nil
}

func Read(path string) (Receipt, error) {
	var r Receipt
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 4096 {
		return r, errors.New("book collection receipt unavailable or not private")
	}
	f, err := os.Open(path)
	if err != nil {
		return r, errors.New("book collection receipt unavailable")
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, 4097))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&r) != nil {
		return r, errors.New("book collection receipt invalid")
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return r, errors.New("book collection receipt has trailing data")
	}
	return r, r.Validate()
}

func Write(path string, r Receipt) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if !filepath.IsAbs(path) {
		return errors.New("book collection receipt path must be absolute")
	}
	if info, err := os.Lstat(path); err == nil && (!info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0) {
		return errors.New("book collection receipt existing path is unsafe")
	} else if err != nil && !os.IsNotExist(err) {
		return errors.New("book collection receipt path unavailable")
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".book-collection-*")
	if err != nil {
		return errors.New("book collection receipt write failed")
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		err = json.NewEncoder(f).Encode(r)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil || os.Rename(f.Name(), path) != nil {
		return errors.New("book collection receipt write failed")
	}
	return nil
}
