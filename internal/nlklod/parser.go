package nlklod

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const (
	rdfNamespace       = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	maxPropertyText    = 64 * 1024
	maxPropertyValues  = 256
	maxValuesPerTag    = 64
	maxResourceValues  = 256 * 1024
	maxResourceTypes   = 64
	maxResourceIDBytes = 4096
)

var namespacePrefixes = map[string]string{
	rdfNamespace:                                    "rdf",
	"http://purl.org/dc/terms/":                     "dcterms",
	"http://purl.org/dc/elements/1.1/":              "dc",
	"http://www.w3.org/2000/01/rdf-schema#":         "rdfs",
	"http://www.w3.org/2002/07/owl#":                "owl",
	"http://www.w3.org/2004/02/skos/core#":          "skos",
	"http://purl.org/ontology/bibo/":                "bibo",
	"http://id.loc.gov/ontologies/bibframe/":        "bibframe",
	"http://lod.nl.go.kr/ontology/":                 "nlon",
	"http://xmlns.com/foaf/0.1/":                    "foaf",
	"http://schema.org/":                            "schema",
	"http://www.w3.org/2003/01/geo/wgs84_pos#":      "geo",
	"http://www.w3.org/XML/1998/namespace":          "xml",
	"http://creativecommons.org/ns#":                "cc",
	"http://www.w3.org/2001/XMLSchema#":             "xsd",
	"http://www.w3.org/ns/org#":                     "org",
	"http://www.geonames.org/ontology#":             "geonames",
	"http://purl.org/NET/c4dm/event.owl#":           "event",
	"http://purl.org/vocommons/voaf#":               "voaf",
	"http://purl.org/vocab/vann/":                   "vann",
	"http://www.w3.org/2003/06/sw-vocab-status/ns#": "vs",
}

type PropertyValue struct {
	Value    string `json:"value,omitempty"`
	Resource string `json:"resource,omitempty"`
	Language string `json:"language,omitempty"`
	Datatype string `json:"datatype,omitempty"`
}

type Resource struct {
	QName      string                     `json:"qname"`
	About      string                     `json:"about,omitempty"`
	RDFTypes   []string                   `json:"rdf_types,omitempty"`
	Properties map[string][]PropertyValue `json:"properties,omitempty"`
	Truncated  bool                       `json:"truncated,omitempty"`
}

// StreamResources decodes each direct child of rdf:RDF independently. It never
// materializes the XML document or an archive member in memory.
func StreamResources(reader io.Reader, handle func(index uint64, resource Resource) error) error {
	if reader == nil {
		return errors.New("nil RDF reader")
	}
	if handle == nil {
		return errors.New("nil resource handler")
	}

	decoder := xml.NewDecoder(reader)
	var (
		inRDF bool
		depth int
		index uint64
	)
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			if !inRDF {
				return errors.New("rdf:RDF root not found")
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("decode RDF token: %w", err)
		}

		switch typed := token.(type) {
		case xml.StartElement:
			if !inRDF {
				if typed.Name.Space == rdfNamespace && typed.Name.Local == "RDF" {
					inRDF = true
					depth = 1
				}
				continue
			}
			if depth == 1 {
				resource, err := decodeResource(decoder, typed)
				if err != nil {
					return err
				}
				if err := handle(index, resource); err != nil {
					return err
				}
				index++
				continue
			}
			depth++
		case xml.EndElement:
			if inRDF {
				depth--
				if depth == 0 {
					return nil
				}
			}
		}
	}
}

func decodeResource(decoder *xml.Decoder, start xml.StartElement) (Resource, error) {
	resource := Resource{
		QName:      QName(start.Name),
		About:      truncateUTF8(attribute(start.Attr, rdfNamespace, "about"), maxResourceIDBytes),
		Properties: make(map[string][]PropertyValue),
	}
	if resource.About == "" {
		resource.About = truncateUTF8(attribute(start.Attr, rdfNamespace, "nodeID"), maxResourceIDBytes)
	}

	totalValues := 0
	totalValueBytes := 0
	for {
		token, err := decoder.Token()
		if err != nil {
			return Resource{}, fmt.Errorf("decode RDF resource: %w", err)
		}
		switch typed := token.(type) {
		case xml.StartElement:
			value, truncated, err := decodeProperty(decoder, typed)
			if err != nil {
				return Resource{}, err
			}
			if truncated {
				resource.Truncated = true
			}
			qname := QName(typed.Name)
			if qname == "rdf:type" {
				rdfType := firstNonEmpty(value.Resource, value.Value)
				if rdfType != "" && len(resource.RDFTypes) < maxResourceTypes {
					resource.RDFTypes = append(resource.RDFTypes, rdfType)
				} else if rdfType != "" {
					resource.Truncated = true
				}
			}
			valueBytes := propertyValueBytes(value)
			if totalValueBytes+valueBytes > maxResourceValues {
				value, truncated = fitPropertyValue(value, maxResourceValues-totalValueBytes)
				resource.Truncated = true
				valueBytes = propertyValueBytes(value)
			}
			if totalValues < maxPropertyValues &&
				len(resource.Properties[qname]) < maxValuesPerTag &&
				(totalValueBytes < maxResourceValues || valueBytes == 0) {
				resource.Properties[qname] = append(resource.Properties[qname], value)
				totalValues++
				totalValueBytes += valueBytes
			} else {
				resource.Truncated = true
			}
		case xml.EndElement:
			if typed.Name == start.Name {
				return resource, nil
			}
		}
	}
}

func propertyValueBytes(value PropertyValue) int {
	return len(value.Value) + len(value.Resource) + len(value.Language) + len(value.Datatype)
}

func fitPropertyValue(value PropertyValue, remaining int) (PropertyValue, bool) {
	if remaining < 0 {
		remaining = 0
	}
	original := propertyValueBytes(value)
	fields := []*string{&value.Value, &value.Resource, &value.Language, &value.Datatype}
	for _, field := range fields {
		if remaining <= 0 {
			*field = ""
			continue
		}
		*field = truncateUTF8(*field, remaining)
		remaining -= len(*field)
	}
	return value, propertyValueBytes(value) < original
}

func decodeProperty(decoder *xml.Decoder, start xml.StartElement) (PropertyValue, bool, error) {
	value := PropertyValue{
		Resource: truncateUTF8(attribute(start.Attr, rdfNamespace, "resource"), maxResourceIDBytes),
		Language: truncateUTF8(attribute(start.Attr, "http://www.w3.org/XML/1998/namespace", "lang"), 128),
		Datatype: truncateUTF8(attribute(start.Attr, rdfNamespace, "datatype"), 1024),
	}
	var builder strings.Builder
	depth := 1
	truncated := false
	for depth > 0 {
		token, err := decoder.Token()
		if err != nil {
			return PropertyValue{}, false, fmt.Errorf("decode RDF property: %w", err)
		}
		switch typed := token.(type) {
		case xml.StartElement:
			depth++
			if value.Resource == "" {
				value.Resource = truncateUTF8(attribute(typed.Attr, rdfNamespace, "resource"), maxResourceIDBytes)
			}
		case xml.CharData:
			if builder.Len() < maxPropertyText {
				remaining := maxPropertyText - builder.Len()
				chunk := []byte(typed)
				if len(chunk) > remaining {
					chunk = chunk[:remaining]
					truncated = true
				}
				builder.Write(chunk)
			} else if len(typed) > 0 {
				truncated = true
			}
		case xml.EndElement:
			depth--
		}
	}
	value.Value = truncateUTF8(builder.String(), maxPropertyText)
	return value, truncated, nil
}

func attribute(attributes []xml.Attr, namespace, local string) string {
	for _, attr := range attributes {
		if attr.Name.Space == namespace && attr.Name.Local == local {
			return strings.TrimSpace(attr.Value)
		}
	}
	return ""
}

func QName(name xml.Name) string {
	if prefix := namespacePrefixes[name.Space]; prefix != "" {
		return prefix + ":" + name.Local
	}
	if name.Space == "" {
		return name.Local
	}
	return "{" + name.Space + "}" + name.Local
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func truncateUTF8(value string, maxBytes int) string {
	value = strings.TrimSpace(value)
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value
	}
	value = value[:maxBytes]
	for len(value) > 0 && !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return strings.TrimSpace(value)
}
