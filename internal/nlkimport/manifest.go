package nlkimport

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const ManifestMaximumFiles = 1000
const ManifestMaximumBytes = 2 << 20
const DriveReadScope = "https://www.googleapis.com/auth/drive.readonly"

type Manifest struct {
	Root  string         `json:"root"`
	Files []ManifestFile `json:"files"`
}

type ManifestFile struct {
	Folder       string `json:"folder"`
	ID           string `json:"id"`
	Name         string `json:"name"`
	Size         uint64 `json:"size"`
	ModifiedTime string `json:"modified_time,omitempty"`
	MD5          string `json:"md5_checksum,omitempty"`
	Revision     string `json:"revision,omitempty"`
}

type ManifestLineage struct {
	DatasetName       string
	SourceArchive     string
	SourceEntry       string
	SourceRevision    string
	UncompressedBytes uint64
	MD5               string
}

func (m Manifest) Lineages(snapshot time.Time) ([]ManifestLineage, error) {
	if snapshot.IsZero() {
		return nil, safeError("snapshot_date_required")
	}
	if err := m.Validate(len(m.Files), 0); err != nil {
		return nil, err
	}
	lineages := make([]ManifestLineage, 0, len(m.Files))
	for _, file := range m.Files {
		dataset := manifestFolderDatasets[file.Folder]
		archive := supportedDatasets[dataset].Stem + "_rdf_" + snapshot.Format("20060102") + ".zip"
		lineages = append(lineages, ManifestLineage{DatasetName: dataset, SourceArchive: archive, SourceEntry: strings.TrimSuffix(archive, ".zip") + "/" + file.Name, SourceRevision: file.Revision, UncompressedBytes: file.Size, MD5: file.MD5})
	}
	sort.Slice(lineages, func(i, j int) bool {
		return lineages[i].SourceArchive+lineages[i].SourceEntry < lineages[j].SourceArchive+lineages[j].SourceEntry
	})
	return lineages, nil
}

func (m Manifest) SHA256() (string, error) {
	if err := m.Validate(len(m.Files), 0); err != nil {
		return "", err
	}
	m.Files = append([]ManifestFile(nil), m.Files...)
	sort.Slice(m.Files, func(i, j int) bool {
		return m.Files[i].Folder+"/"+m.Files[i].Name < m.Files[j].Folder+"/"+m.Files[j].Name
	})
	body, err := json.Marshal(m)
	if err != nil {
		return "", safeError("manifest_invalid")
	}
	return fmt.Sprintf("%x", sha256.Sum256(body)), nil
}

var driveIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{10,200}$`)
var md5Pattern = regexp.MustCompile(`^[a-f0-9]{32}$`)
var manifestFolderDatasets = map[string]string{
	"book": "book", "Concept": "concept", "Person": "person", "Library": "library", "Organization": "organization",
	"Offline": "offline", "Online": "online", "audiovisual": "audiovisual", "government": "government_publication", "serial": "serial", "thesis": "thesis",
}

var snapshot20260529Files = map[string]int{"book": 20, "Concept": 3, "Person": 10, "Library": 1, "Organization": 1, "Offline": 36, "Online": 110, "audiovisual": 10, "government": 4, "serial": 2, "thesis": 11}

func LoadManifest(path string, expectedFiles int, expectedBytes uint64) (Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return Manifest{}, safeError("manifest_unavailable")
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, ManifestMaximumBytes+1))
	if err != nil || len(body) > ManifestMaximumBytes {
		return Manifest{}, safeError("manifest_size")
	}
	var manifest Manifest
	if json.Unmarshal(body, &manifest) != nil {
		return Manifest{}, safeError("manifest_invalid")
	}
	if err := manifest.Validate(expectedFiles, expectedBytes); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func (m Manifest) Validate(expectedFiles int, expectedBytes uint64) error {
	if expectedFiles < 1 || expectedFiles > ManifestMaximumFiles || len(m.Files) != expectedFiles || !driveIDPattern.MatchString(m.Root) {
		return safeError("manifest_coverage")
	}
	seenIDs, seenPaths := map[string]bool{}, map[string]bool{}
	var total uint64
	for _, f := range m.Files {
		dataset, ok := manifestFolderDatasets[f.Folder]
		if !ok || !driveIDPattern.MatchString(f.ID) || f.Size == 0 || f.Size > 16<<30 || filepath.Base(f.Name) != f.Name {
			return safeError("manifest_file_identity")
		}
		stem := supportedDatasets[dataset].Stem
		if !regexp.MustCompile(`^` + regexp.QuoteMeta(stem) + `_[0-9]+[.]rdf$`).MatchString(f.Name) {
			return safeError("manifest_file_name")
		}
		key := f.Folder + "/" + f.Name
		if seenIDs[f.ID] || seenPaths[key] {
			return safeError("manifest_duplicate")
		}
		seenIDs[f.ID] = true
		seenPaths[key] = true
		if f.MD5 != "" && !md5Pattern.MatchString(f.MD5) {
			return safeError("manifest_checksum")
		}
		total += f.Size
	}
	if expectedBytes > 0 && total != expectedBytes {
		return safeError("manifest_byte_coverage")
	}
	if expectedFiles == 208 {
		for folder, count := range snapshot20260529Files {
			stem := supportedDatasets[manifestFolderDatasets[folder]].Stem
			for index := 0; index < count; index++ {
				if !seenPaths[fmt.Sprintf("%s/%s_%d.rdf", folder, stem, index)] {
					return safeError("manifest_dataset_coverage")
				}
			}
		}
	}
	return nil
}

// DriveSource only speaks to the official Google endpoints. Tokens and service
// account JSON are never included in an error, a checkpoint, or an API URL.
type DriveSource struct {
	client *http.Client
	base   string
}

func NewDriveSourceFromEnv(ctx context.Context) (*DriveSource, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	transport.IdleConnTimeout = 60 * time.Second
	baseClient := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	authCtx := context.WithValue(ctx, oauth2.HTTPClient, baseClient)
	var source oauth2.TokenSource
	if token := strings.TrimSpace(os.Getenv("NLK_GOOGLE_DRIVE_ACCESS_TOKEN")); token != "" {
		if strings.ContainsAny(token, "\r\n") {
			return nil, safeError("drive_credentials")
		}
		source = oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token, TokenType: "Bearer"})
	} else {
		body := []byte(os.Getenv("NLK_GOOGLE_SERVICE_ACCOUNT_JSON"))
		if path := strings.TrimSpace(os.Getenv("NLK_GOOGLE_SERVICE_ACCOUNT_FILE")); len(body) == 0 && path != "" {
			f, err := os.Open(path)
			if err != nil {
				return nil, safeError("drive_credentials")
			}
			body, err = io.ReadAll(io.LimitReader(f, 64*1024+1))
			f.Close()
			if err != nil {
				return nil, safeError("drive_credentials")
			}
		}
		if len(body) == 0 || len(body) > 64*1024 {
			return nil, safeError("drive_credentials_required")
		}
		var identity struct {
			Type     string `json:"type"`
			TokenURI string `json:"token_uri"`
		}
		if json.Unmarshal(body, &identity) != nil || identity.Type != "service_account" || identity.TokenURI != "https://oauth2.googleapis.com/token" {
			return nil, safeError("drive_credentials")
		}
		cfg, err := google.JWTConfigFromJSON(body, DriveReadScope)
		if err != nil {
			return nil, safeError("drive_credentials")
		}
		source = cfg.TokenSource(authCtx)
	}
	client := &http.Client{Transport: &oauth2.Transport{Source: oauth2.ReuseTokenSource(nil, source), Base: transport}, CheckRedirect: baseClient.CheckRedirect}
	return &DriveSource{client: client, base: "https://www.googleapis.com/drive/v3/files"}, nil
}

func (s *DriveSource) get(ctx context.Context, suffix string, params url.Values) (*http.Response, error) {
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+suffix+"?"+params.Encode(), nil)
		if err != nil {
			return nil, safeError("drive_request")
		}
		resp, err := s.client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		retry := err != nil
		if resp != nil {
			retry = resp.StatusCode == 429 || resp.StatusCode >= 500
			resp.Body.Close()
		}
		if !retry || attempt == 2 {
			return nil, safeError("drive_request_failed")
		}
		timer := time.NewTimer(time.Duration(attempt+1) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, safeError("cancelled")
		case <-timer.C:
		}
	}
	return nil, safeError("drive_request_failed")
}

type driveMetadata struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Size     string `json:"size"`
	MD5      string `json:"md5Checksum"`
	Modified string `json:"modifiedTime"`
	Version  string `json:"version"`
	Mime     string `json:"mimeType"`
	Trashed  bool   `json:"trashed"`
}

func (s *DriveSource) metadata(ctx context.Context, f ManifestFile) (ManifestFile, error) {
	if !driveIDPattern.MatchString(f.ID) {
		return ManifestFile{}, safeError("manifest_file_identity")
	}
	resp, err := s.get(ctx, "/"+f.ID, url.Values{"fields": {"id,name,size,md5Checksum,modifiedTime,version,mimeType,trashed"}, "supportsAllDrives": {"true"}})
	if err != nil {
		return ManifestFile{}, err
	}
	defer resp.Body.Close()
	var data driveMetadata
	if json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&data) != nil {
		return ManifestFile{}, safeError("drive_metadata")
	}
	size, err := strconv.ParseUint(data.Size, 10, 64)
	if err != nil || size != f.Size || data.ID != f.ID || data.Name != f.Name || data.Trashed || !md5Pattern.MatchString(data.MD5) || data.Version == "" {
		return ManifestFile{}, safeError("drive_source_changed")
	}
	if f.MD5 != "" && f.MD5 != data.MD5 {
		return ManifestFile{}, safeError("drive_source_changed")
	}
	if f.ModifiedTime != "" && f.ModifiedTime != data.Modified {
		return ManifestFile{}, safeError("drive_source_changed")
	}
	f.MD5 = data.MD5
	f.ModifiedTime = data.Modified
	revision := "gdrive:" + f.ID + ":" + data.Version + ":" + data.MD5 + ":" + data.Size
	if f.Revision != "" && f.Revision != revision {
		return ManifestFile{}, safeError("drive_source_changed")
	}
	f.Revision = revision
	return f, nil
}

func (s *DriveSource) Discover(ctx context.Context, root string, expectedFiles int, expectedBytes uint64) (Manifest, error) {
	if !driveIDPattern.MatchString(root) {
		return Manifest{}, safeError("drive_folder_id")
	}
	list := func(parent string) ([]driveMetadata, error) {
		var files []driveMetadata
		page := ""
		for pages := 0; pages < 20; pages++ {
			params := url.Values{"q": {"'" + parent + "' in parents and trashed = false"}, "pageSize": {"1000"}, "fields": {"nextPageToken,files(id,name,size,md5Checksum,modifiedTime,version,mimeType,trashed)"}, "supportsAllDrives": {"true"}, "includeItemsFromAllDrives": {"true"}}
			if page != "" {
				params.Set("pageToken", page)
			}
			resp, err := s.get(ctx, "", params)
			if err != nil {
				return nil, err
			}
			var data struct {
				Next  string          `json:"nextPageToken"`
				Files []driveMetadata `json:"files"`
			}
			err = json.NewDecoder(io.LimitReader(resp.Body, ManifestMaximumBytes)).Decode(&data)
			resp.Body.Close()
			if err != nil {
				return nil, safeError("drive_inventory")
			}
			files = append(files, data.Files...)
			if len(files) > ManifestMaximumFiles {
				return nil, safeError("drive_inventory_bound")
			}
			if data.Next == "" {
				return files, nil
			}
			if data.Next == page {
				return nil, safeError("drive_inventory_pagination")
			}
			page = data.Next
		}
		return nil, safeError("drive_inventory_bound")
	}
	folders, err := list(root)
	if err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{Root: root}
	seen := map[string]bool{}
	for _, folder := range folders {
		if _, ok := manifestFolderDatasets[folder.Name]; !ok || folder.Mime != "application/vnd.google-apps.folder" || seen[folder.Name] {
			return Manifest{}, safeError("drive_folder_inventory")
		}
		if !driveIDPattern.MatchString(folder.ID) {
			return Manifest{}, safeError("drive_folder_inventory")
		}
		seen[folder.Name] = true
		files, err := list(folder.ID)
		if err != nil {
			return Manifest{}, err
		}
		for _, file := range files {
			size, err := strconv.ParseUint(file.Size, 10, 64)
			if err != nil || !md5Pattern.MatchString(file.MD5) || file.Version == "" {
				return Manifest{}, safeError("drive_inventory")
			}
			manifest.Files = append(manifest.Files, ManifestFile{Folder: folder.Name, ID: file.ID, Name: file.Name, Size: size, ModifiedTime: file.Modified, MD5: file.MD5, Revision: "gdrive:" + file.ID + ":" + file.Version + ":" + file.MD5 + ":" + file.Size})
		}
	}
	if len(seen) != len(manifestFolderDatasets) {
		return Manifest{}, safeError("drive_folder_coverage")
	}
	if err := manifest.Validate(expectedFiles, expectedBytes); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

type verifiedEntryReader struct {
	io.ReadCloser
	expected    uint64
	expectedMD5 string
	read        uint64
	md5         hash.Hash
	crc         hash.Hash32
}

func newVerifiedEntryReader(reader io.ReadCloser, size uint64, checksum string) *verifiedEntryReader {
	return &verifiedEntryReader{ReadCloser: reader, expected: size, expectedMD5: checksum, md5: md5.New(), crc: crc32.NewIEEE()}
}
func (r *verifiedEntryReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.read += uint64(n)
	r.md5.Write(p[:n])
	r.crc.Write(p[:n])
	if r.read > r.expected {
		return n, safeError("entry_size_mismatch")
	}
	return n, err
}
func (r *verifiedEntryReader) Verify() error {
	if r.read != r.expected {
		return safeError("entry_size_mismatch")
	}
	if r.expectedMD5 != "" && hex.EncodeToString(r.md5.Sum(nil)) != r.expectedMD5 {
		return safeError("entry_checksum_failed")
	}
	return nil
}

func discoverManifestPlans(ctx context.Context, config Config) ([]archivePlan, error) {
	if config.Manifest == nil {
		return nil, safeError("manifest_required")
	}
	if err := config.Manifest.Validate(len(config.Manifest.Files), 0); err != nil {
		return nil, err
	}
	byDataset := map[string]*archivePlan{}
	for _, file := range config.Manifest.Files {
		dataset := manifestFolderDatasets[file.Folder]
		spec := supportedDatasets[dataset]
		if config.DriveSource != nil {
			verified, err := config.DriveSource.metadata(ctx, file)
			if err != nil {
				return nil, err
			}
			file = verified
		} else {
			path, err := containedManifestPath(config.InputDir, file, config.SnapshotDate)
			if err != nil {
				return nil, err
			}
			info, err := os.Stat(path)
			if err != nil || !info.Mode().IsRegular() || uint64(info.Size()) != file.Size {
				return nil, safeError("manifest_local_file")
			}
			stream, err := os.Open(path)
			if err != nil {
				return nil, safeError("manifest_local_file")
			}
			hasher := md5.New()
			size, readErr := io.Copy(hasher, contextManifestReader{ctx: ctx, reader: stream})
			_ = stream.Close()
			checksum := hex.EncodeToString(hasher.Sum(nil))
			if readErr != nil || uint64(size) != file.Size || (file.MD5 != "" && file.MD5 != checksum) {
				return nil, safeError("manifest_local_checksum")
			}
			file.MD5 = checksum
			file.Revision = fmt.Sprintf("local:%s:%d", checksum, file.Size)
		}
		archive := spec.Stem + "_rdf_" + config.SnapshotDate.Format("20060102") + ".zip"
		plan := byDataset[dataset]
		if plan == nil {
			plan = &archivePlan{Dataset: dataset, BaseName: archive}
			byDataset[dataset] = plan
		}
		entry := entryPlan{Name: strings.TrimSuffix(archive, ".zip") + "/" + file.Name, UncompressedBytes: file.Size, DirectFile: &file}
		plan.Entries = append(plan.Entries, entry)
		plan.UncompressedBytes += file.Size
	}
	plans := make([]archivePlan, 0, len(byDataset))
	for _, plan := range byDataset {
		sort.Slice(plan.Entries, func(i, j int) bool { return plan.Entries[i].Name < plan.Entries[j].Name })
		plans = append(plans, *plan)
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].Dataset < plans[j].Dataset })
	return plans, nil
}

// VerifyManifest resolves immutable source revisions without database access.
// The returned inventory is suitable for both import and publication coverage.
func VerifyManifest(ctx context.Context, config Config) (Manifest, error) {
	if config.Manifest == nil || config.SnapshotDate.IsZero() {
		return Manifest{}, safeError("manifest_required")
	}
	plans, err := discoverManifestPlans(ctx, config)
	if err != nil {
		return Manifest{}, err
	}
	verified := Manifest{Root: config.Manifest.Root, Files: make([]ManifestFile, 0, len(config.Manifest.Files))}
	for _, plan := range plans {
		for _, entry := range plan.Entries {
			verified.Files = append(verified.Files, *entry.DirectFile)
		}
	}
	return verified, nil
}

type contextManifestReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextManifestReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func containedManifestPath(root string, file ManifestFile, snapshot time.Time) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", safeError("input_directory_required")
	}
	actualRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", safeError("manifest_local_file")
	}
	dataset, ok := manifestFolderDatasets[file.Folder]
	if !ok || snapshot.IsZero() {
		return "", safeError("manifest_file_identity")
	}
	// Accept the Drive folder layout and the exact official extracted ZIP
	// layout, while retaining the original archive/entry checkpoint identity.
	folders := []string{file.Folder, supportedDatasets[dataset].Stem + "_rdf_" + snapshot.Format("20060102")}
	selected := ""
	for _, folder := range folders {
		candidate := filepath.Join(actualRoot, folder, file.Name)
		if _, err := os.Lstat(candidate); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return "", safeError("manifest_local_file")
		}
		path, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			return "", safeError("manifest_local_file")
		}
		rel, err := filepath.Rel(actualRoot, path)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return "", safeError("manifest_local_path")
		}
		if selected != "" && selected != path {
			return "", safeError("manifest_local_ambiguous")
		}
		selected = path
	}
	if selected == "" {
		return "", safeError("manifest_local_file")
	}
	return selected, nil
}

func openManifestEntry(ctx context.Context, config Config, file ManifestFile) (*verifiedEntryReader, error) {
	var stream io.ReadCloser
	if config.DriveSource != nil {
		verified, err := config.DriveSource.metadata(ctx, file)
		if err != nil {
			return nil, err
		}
		if verified.Revision != file.Revision {
			return nil, safeError("drive_source_changed")
		}
		resp, err := config.DriveSource.get(ctx, "/"+file.ID, url.Values{"alt": {"media"}, "supportsAllDrives": {"true"}})
		if err != nil {
			return nil, err
		}
		if resp.ContentLength >= 0 && uint64(resp.ContentLength) != file.Size {
			resp.Body.Close()
			return nil, safeError("entry_size_mismatch")
		}
		// Verify a single bounded RDF file before any row can advance a durable
		// checkpoint. The temporary file is deleted on close; the full snapshot
		// is never materialized on disk.
		cache, err := os.CreateTemp(os.Getenv("NLK_RDF_CACHE_DIR"), "nlk-rdf-*")
		if err != nil {
			resp.Body.Close()
			return nil, safeError("entry_cache_unavailable")
		}
		cleanup := func() { cache.Close(); os.Remove(cache.Name()) }
		verifiedBody := newVerifiedEntryReader(resp.Body, file.Size, file.MD5)
		_, copyErr := io.Copy(cache, verifiedBody)
		verifyErr := verifiedBody.Verify()
		_ = verifiedBody.Close()
		if copyErr != nil || verifyErr != nil {
			cleanup()
			return nil, safeError("entry_checksum_failed")
		}
		if _, err := cache.Seek(0, io.SeekStart); err != nil {
			cleanup()
			return nil, safeError("entry_cache_unavailable")
		}
		stream = &temporaryManifestFile{File: cache}
	} else {
		path, err := containedManifestPath(config.InputDir, file, config.SnapshotDate)
		if err != nil {
			return nil, err
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, safeError("entry_open_failed")
		}
		stream = f
	}
	return newVerifiedEntryReader(stream, file.Size, file.MD5), nil
}

type temporaryManifestFile struct{ *os.File }

func (f *temporaryManifestFile) Close() error {
	err := f.File.Close()
	removeErr := os.Remove(f.Name())
	if err != nil {
		return err
	}
	return removeErr
}
