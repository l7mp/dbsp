package circuit

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	nodeIDSanitizer       = regexp.MustCompile(`[^a-zA-Z0-9_]+`)
	topicSegmentSanitizer = regexp.MustCompile(`[^a-z0-9._-]+`)
)

// SanitizeNodeID normalizes a string for use in circuit node IDs.
func SanitizeNodeID(s string) string {
	s = nodeIDSanitizer.ReplaceAllString(s, "_")
	s = strings.Trim(s, "_")
	if s == "" {
		return "unnamed"
	}
	return s
}

// InputNodeID returns the canonical circuit input node ID.
func InputNodeID(name string) string {
	return "input_" + SanitizeNodeID(name)
}

// OutputNodeID returns the canonical circuit output node ID.
func OutputNodeID(name string) string {
	return "output_" + SanitizeNodeID(name)
}

// InputTopic returns the canonical runtime input topic.
func InputTopic(namespace, name string) string {
	return fmt.Sprintf("%s/%s/input", sanitizeTopicSegment(namespace), sanitizeTopicSegment(name))
}

// OutputTopic returns the canonical runtime output topic.
func OutputTopic(namespace, name string) string {
	return fmt.Sprintf("%s/%s/output", sanitizeTopicSegment(namespace), sanitizeTopicSegment(name))
}

func sanitizeTopicSegment(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = topicSegmentSanitizer.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-._")
	if s == "" {
		return "unnamed"
	}
	return s
}
