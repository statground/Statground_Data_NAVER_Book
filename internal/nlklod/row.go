package nlklod

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"statground_naver_book_go/internal/bookmodel"
	"statground_naver_book_go/internal/util"
)

const (
	SourceURL           = "https://lod.nl.go.kr/home/dataset/datadownload.do"
	LicenseName         = "공공누리 제1유형; CC0 1.0"
	LicenseURL          = "https://lod.nl.go.kr/home/dataset/datadownload.do"
	Attribution         = "국립중앙도서관 국가서지 LOD"
	MaxPayloadJSONBytes = 64 * 1024
)

var fourDigitYear = regexp.MustCompile(`(?:^|[^0-9])([12][0-9]{3})(?:[^0-9]|$)`)

type Evidence struct {
	RunUUID       string
	DatasetName   string
	SnapshotDate  time.Time
	SourceArchive string
	SourceEntry   string
	RecordIndex   uint64
	ImportedAt    time.Time
}

type IDGenerator func() string

func BuildRow(resource Resource, evidence Evidence) (map[string]any, error) {
	if strings.TrimSpace(evidence.RunUUID) == "" {
		return nil, fmt.Errorf("run UUID is required")
	}
	if evidence.SnapshotDate.IsZero() {
		return nil, fmt.Errorf("snapshot date is required")
	}
	if evidence.ImportedAt.IsZero() {
		evidence.ImportedAt = util.NowKST()
	}

	resourceID := strings.TrimSpace(resource.About)
	if resourceID == "" {
		resourceID = fallbackResourceID(evidence)
	}
	payloadJSON, err := boundedPayloadJSON(resource, resourceID)
	if err != nil {
		return nil, err
	}

	isbnRaw := propertyTexts(resource, "bibo:isbn")
	isbn10 := make([]string, 0, len(isbnRaw))
	isbn13 := make([]string, 0, len(isbnRaw))
	canonicalISBNs := make([]string, 0, len(isbnRaw))
	// Keep this as []int rather than []uint8: encoding/json treats []uint8 as
	// binary data and would emit base64 instead of a JSON array for
	// ClickHouse Array(UInt8).
	isbnValid := make([]int, 0, len(isbnRaw))
	for _, raw := range isbnRaw {
		identity := bookmodel.NormalizeISBN(raw)
		isbn10 = append(isbn10, identity.ISBN10)
		isbn13 = append(isbn13, identity.ISBN13)
		canonicalISBNs = append(canonicalISBNs, identity.CanonicalISBN)
		if identity.Valid {
			isbnValid = append(isbnValid, 1)
		} else {
			isbnValid = append(isbnValid, 0)
		}
	}

	issuedRaw := firstPropertyText(resource, "dcterms:issued")
	issuedYear := parseIssuedYear(firstPropertyText(resource, "nlon:issuedYear"))
	if issuedYear == nil {
		issuedYear = parseIssuedYear(issuedRaw)
	}
	snapshot := time.Date(
		evidence.SnapshotDate.Year(),
		evidence.SnapshotDate.Month(),
		evidence.SnapshotDate.Day(),
		0, 0, 0, 0,
		util.KST(),
	)
	stable := map[string]any{
		"resource_id":           resourceID,
		"resource_type":         normalizeResourceType(resource, evidence.DatasetName),
		"dataset_name":          evidence.DatasetName,
		"dataset_snapshot_date": snapshot.Format("2006-01-02"),
		"source_archive":        evidence.SourceArchive,
		"source_entry":          evidence.SourceEntry,
		"source_record_index":   evidence.RecordIndex,
		"title":                 firstPropertyText(resource, "dcterms:title"),
		"label":                 firstNonEmpty(firstPropertyText(resource, "rdfs:label"), firstPropertyText(resource, "skos:prefLabel")),
		"description":           firstNonEmpty(firstPropertyText(resource, "dcterms:description"), firstPropertyText(resource, "dcterms:abstract")),
		"creators":              propertyTexts(resource, "dc:creator"),
		"creator_ids":           propertyResources(resource, "dcterms:creator"),
		"publisher":             firstPropertyText(resource, "dc:publisher"),
		"issued_raw":            issuedRaw,
		"issued_year":           issuedYear,
		"languages":             combine(propertyValues(resource, "bibframe:language"), propertyValues(resource, "dcterms:language")),
		"publication_places":    combine(propertyValues(resource, "nlon:publicationPlace"), propertyValues(resource, "bibframe:place")),
		"keywords":              propertyTexts(resource, "nlon:keyword"),
		"subject_ids":           propertyResources(resource, "dcterms:subject"),
		"kdc_codes":             combine(propertyValues(resource, "nlon:kdc"), propertyValues(resource, "nlon:classificationNumberOfNLK"), propertyValues(resource, "nlon:kdcn")),
		"ddc_codes":             combine(propertyValues(resource, "nlon:ddc"), propertyValues(resource, "nlon:classificationNumberOfDDC"), propertyValues(resource, "nlon:ddcn")),
		"series_titles":         propertyTexts(resource, "nlon:titleOfSeries"),
		"extents":               propertyTexts(resource, "bibframe:extent"),
		"related_resource_ids":  combine(propertyResources(resource, "dcterms:relation"), propertyValues(resource, "rdfs:seeAlso"), propertyResources(resource, "nlon:create")),
		"isbn_raw":              isbnRaw,
		"isbn10":                isbn10,
		"isbn13":                isbn13,
		"canonical_isbns":       canonicalISBNs,
		"isbn_valid":            isbnValid,
		"alt_labels":            combine(propertyTexts(resource, "skos:altLabel"), propertyTexts(resource, "dcterms:alternative")),
		"same_as_ids":           propertyValues(resource, "owl:sameAs"),
		"birth_raw":             firstPropertyText(resource, "nlon:birthYear"),
		"death_raw":             firstPropertyText(resource, "nlon:deathYear"),
		"jobs":                  propertyTexts(resource, "schema:jobTitle"),
		"fields":                propertyTexts(resource, "nlon:fieldOfActivity"),
		"isni_ids":              propertyValues(resource, "nlon:isni"),
		"broader_ids":           propertyResources(resource, "skos:broader"),
		"related_ids":           combine(propertyResources(resource, "skos:related"), propertyResources(resource, "skos:narrower")),
		"library_identifier":    firstPropertyText(resource, "dcterms:identifier"),
		"library_type":          firstPropertyValue(resource, "nlon:libraryType"),
		"library_homepage":      firstPropertyValue(resource, "foaf:homepage"),
		"library_phone":         firstPropertyValue(resource, "foaf:phone"),
		"library_fax":           firstPropertyValue(resource, "schema:faxNumber"),
		"library_opening_year":  firstNonEmpty(firstPropertyText(resource, "nlon:openingYear"), firstPropertyText(resource, "nlon:dateOfOpening")),
		"library_closed_days":   propertyTexts(resource, "nlon:dateOfClosed"),
		"library_summer_hours":  propertyTexts(resource, "nlon:summerOpenTime"),
		"library_winter_hours":  propertyTexts(resource, "nlon:winterOpenTime"),
		"library_locations":     libraryLocations(resource),
		"payload_json":          payloadJSON,
	}
	stableJSON, err := json.Marshal(stable)
	if err != nil {
		return nil, fmt.Errorf("encode normalized NLK resource: %w", err)
	}
	sum := sha256.Sum256(stableJSON)
	contentHash := hex.EncodeToString(sum[:])

	row := make(map[string]any, len(stable)+14)
	for key, value := range stable {
		row[key] = value
	}
	row["uuid"] = deterministicObservationUUID(snapshot, evidence, contentHash)
	row["run_uuid"] = evidence.RunUUID
	row["version"] = deterministicVersion(snapshot, evidence, contentHash)
	row["dataset_updated_at"] = util.FormatCHDateTime64Millis(snapshot)
	row["source_format"] = "rdf_xml"
	row["source_url"] = SourceURL
	row["license_name"] = LicenseName
	row["license_url"] = LicenseURL
	row["attribution"] = Attribution
	row["content_hash"] = contentHash
	row["imported_at"] = util.FormatCHDateTime64Millis(evidence.ImportedAt)
	return row, nil
}

func deterministicObservationUUID(snapshot time.Time, evidence Evidence, contentHash string) string {
	version := deterministicVersion(snapshot, evidence, contentHash)
	milliseconds := version / 1000
	seed := strings.Join([]string{
		evidence.DatasetName,
		evidence.SourceArchive,
		evidence.SourceEntry,
		strconv.FormatUint(evidence.RecordIndex, 10),
		contentHash,
		"uuidv7",
	}, "\x1f")
	sum := sha256.Sum256([]byte(seed))
	var value [16]byte
	value[0] = byte(milliseconds >> 40)
	value[1] = byte(milliseconds >> 32)
	value[2] = byte(milliseconds >> 24)
	value[3] = byte(milliseconds >> 16)
	value[4] = byte(milliseconds >> 8)
	value[5] = byte(milliseconds)
	copy(value[6:], sum[:10])
	value[6] = (value[6] & 0x0f) | 0x70
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf(
		"%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		value[0], value[1], value[2], value[3],
		value[4], value[5],
		value[6], value[7],
		value[8], value[9],
		value[10], value[11], value[12], value[13], value[14], value[15],
	)
}

func boundedPayloadJSON(resource Resource, resourceID string) (string, error) {
	resource.About = resourceID
	payload, err := json.Marshal(resource)
	if err != nil {
		return "", fmt.Errorf("encode bounded NLK payload: %w", err)
	}
	if len(payload) <= MaxPayloadJSONBytes {
		return string(payload), nil
	}

	counts := make(map[string]int, len(resource.Properties))
	for key, values := range resource.Properties {
		counts[key] = len(values)
	}
	summary := struct {
		QName          string         `json:"qname"`
		About          string         `json:"about,omitempty"`
		RDFTypes       []string       `json:"rdf_types,omitempty"`
		PropertyCounts map[string]int `json:"property_counts,omitempty"`
		Truncated      bool           `json:"truncated"`
	}{
		QName:          truncateUTF8(resource.QName, 1024),
		About:          truncateUTF8(resourceID, maxResourceIDBytes),
		RDFTypes:       boundedStrings(resource.RDFTypes, 32, 2048),
		PropertyCounts: counts,
		Truncated:      true,
	}
	payload, err = json.Marshal(summary)
	if err != nil {
		return "", fmt.Errorf("encode summarized NLK payload: %w", err)
	}
	if len(payload) > MaxPayloadJSONBytes {
		return "", fmt.Errorf("bounded NLK payload exceeds limit")
	}
	return string(payload), nil
}

func normalizeResourceType(resource Resource, datasetName string) string {
	candidates := append([]string{resource.QName}, resource.RDFTypes...)
	for _, candidate := range candidates {
		local := strings.ToLower(candidate)
		if slash := strings.LastIndexAny(local, "/#}:"); slash >= 0 {
			local = local[slash+1:]
		}
		switch local {
		case "book":
			return "book"
		case "offlinematerial":
			return "offline_material"
		case "onlinematerial", "alternativematerial":
			return "online_material"
		case "sound", "video", "image", "cartographic", "audiovisual":
			return "audiovisual"
		case "governmentpublication", "government_publication":
			return "government_publication"
		case "serial", "continuingresource":
			return "serial"
		case "thesis", "dissertation":
			return "thesis"
		case "author", "person":
			return "person"
		case "organization", "corporatebody":
			return "organization"
		case "concept":
			return "concept"
		case "library":
			return "library"
		}
	}
	switch strings.ToLower(strings.TrimSpace(datasetName)) {
	case "book":
		return "book"
	case "offline":
		return "offline_material"
	case "online":
		return "online_material"
	case "audiovisual":
		return "audiovisual"
	case "government_publication", "governmentpublication", "govermentpublication":
		return "government_publication"
	case "serial":
		return "serial"
	case "thesis":
		return "thesis"
	case "person":
		return "person"
	case "organization":
		return "organization"
	case "concept":
		return "concept"
	case "library":
		return "library"
	default:
		return "other"
	}
}

func deterministicVersion(snapshot time.Time, evidence Evidence, contentHash string) uint64 {
	seed := strings.Join([]string{
		evidence.DatasetName,
		evidence.SourceArchive,
		evidence.SourceEntry,
		strconv.FormatUint(evidence.RecordIndex, 10),
		contentHash,
	}, "\x1f")
	sum := sha256.Sum256([]byte(seed))
	const microsPerDay = uint64(24 * time.Hour / time.Microsecond)
	base := uint64(snapshot.UnixMicro())
	return base + binary.BigEndian.Uint64(sum[:8])%microsPerDay
}

func fallbackResourceID(evidence Evidence) string {
	seed := strings.Join([]string{
		evidence.SnapshotDate.Format("2006-01-02"),
		evidence.DatasetName,
		evidence.SourceArchive,
		evidence.SourceEntry,
		strconv.FormatUint(evidence.RecordIndex, 10),
	}, "\x1f")
	sum := sha256.Sum256([]byte(seed))
	return "urn:statground:nlk:" + hex.EncodeToString(sum[:])
}

func parseIssuedYear(value string) any {
	match := fourDigitYear.FindStringSubmatch(strings.TrimSpace(value))
	if len(match) != 2 {
		return nil
	}
	year, err := strconv.ParseUint(match[1], 10, 16)
	if err != nil || year < 1000 || year > 2999 {
		return nil
	}
	return uint16(year)
}

func propertyValues(resource Resource, qname string) []string {
	values := resource.Properties[qname]
	out := make([]string, 0, len(values))
	for _, value := range values {
		if cleaned := firstNonEmpty(value.Resource, value.Value); cleaned != "" {
			out = append(out, cleaned)
		}
	}
	return out
}

func propertyTexts(resource Resource, qname string) []string {
	values := resource.Properties[qname]
	out := make([]string, 0, len(values))
	for _, value := range values {
		if cleaned := strings.TrimSpace(value.Value); cleaned != "" {
			out = append(out, cleaned)
		}
	}
	return out
}

func propertyResources(resource Resource, qname string) []string {
	values := resource.Properties[qname]
	out := make([]string, 0, len(values))
	for _, value := range values {
		if cleaned := strings.TrimSpace(value.Resource); cleaned != "" {
			out = append(out, cleaned)
		}
	}
	return out
}

func firstPropertyText(resource Resource, qname string) string {
	values := propertyTexts(resource, qname)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func firstPropertyValue(resource Resource, qname string) string {
	values := propertyValues(resource, qname)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func combine(groups ...[]string) []string {
	total := 0
	for _, group := range groups {
		total += len(group)
	}
	out := make([]string, 0, total)
	for _, group := range groups {
		out = append(out, group...)
	}
	return out
}

func libraryLocations(resource Resource) []string {
	return combine(
		propertyValues(resource, "geo:location"),
		propertyValues(resource, "schema:address"),
		propertyValues(resource, "nlon:location"),
		propertyValues(resource, "geo:lat"),
		propertyValues(resource, "geo:long"),
	)
}

func boundedStrings(values []string, maxValues, maxBytes int) []string {
	if len(values) > maxValues {
		values = values[:maxValues]
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, truncateUTF8(value, maxBytes))
	}
	return out
}
