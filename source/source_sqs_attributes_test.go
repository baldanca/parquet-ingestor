package source

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// --- extractSQSAttributes ---

func TestExtractSQSAttributes_AllFields(t *testing.T) {
	m := &sqstypes.Message{
		MessageId: aws.String("msg-1"),
		Attributes: map[string]string{
			"ApproximateReceiveCount": "3",
			"SentTimestamp":           "1700000000000",
		},
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"TraceId": {DataType: aws.String("String"), StringValue: aws.String("abc-123")},
			"Count":   {DataType: aws.String("Number"), StringValue: aws.String("42")},
		},
	}

	got := extractSQSAttributes(m)
	if got == nil {
		t.Fatal("expected non-nil attributes map")
	}
	check := func(key, want string) {
		t.Helper()
		if v, ok := got[key]; !ok || v != want {
			t.Errorf("attrs[%q] = %q, want %q", key, v, want)
		}
	}
	check("ApproximateReceiveCount", "3")
	check("SentTimestamp", "1700000000000")
	check("TraceId", "abc-123")
	check("Count", "42")
	check("MessageId", "msg-1")
}

func TestExtractSQSAttributes_EmptyMessage_ReturnsNil(t *testing.T) {
	m := &sqstypes.Message{}
	if got := extractSQSAttributes(m); got != nil {
		t.Fatalf("expected nil for empty message, got %v", got)
	}
}

func TestExtractSQSAttributes_BinaryAttributeSkipped(t *testing.T) {
	// Binary MessageAttributes have no StringValue; they must be silently skipped.
	m := &sqstypes.Message{
		MessageId: aws.String("msg-bin"),
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"Blob": {DataType: aws.String("Binary"), BinaryValue: []byte{0x01, 0x02}},
		},
	}

	got := extractSQSAttributes(m)
	if got == nil {
		t.Fatal("expected non-nil map (MessageId is set)")
	}
	if _, present := got["Blob"]; present {
		t.Fatal("binary attribute must not be included in the map")
	}
	if got["MessageId"] != "msg-bin" {
		t.Fatalf("MessageId = %q, want %q", got["MessageId"], "msg-bin")
	}
}

func TestExtractSQSAttributes_MessageIdOnly(t *testing.T) {
	m := &sqstypes.Message{MessageId: aws.String("only-id")}
	got := extractSQSAttributes(m)
	if got == nil {
		t.Fatal("expected non-nil map")
	}
	if got["MessageId"] != "only-id" {
		t.Fatalf("MessageId = %q, want %q", got["MessageId"], "only-id")
	}
	if len(got) != 1 {
		t.Fatalf("map length = %d, want 1", len(got))
	}
}

// --- message.Data ---

func newTestMessage(src *SourceSQS, msg *sqstypes.Message) *message {
	return &message{src: src, m: msg}
}

func newMinimalSource(includeAttributes bool) *SourceSQS {
	return &SourceSQS{cfg: SourceSQSConfig{IncludeAttributes: includeAttributes}}
}

func TestMessage_Data_WithoutIncludeAttributes(t *testing.T) {
	src := newMinimalSource(false)
	msg := &sqstypes.Message{
		Body:      aws.String("hello"),
		MessageId: aws.String("id-1"),
		Attributes: map[string]string{
			"ApproximateReceiveCount": "1",
		},
	}
	env := newTestMessage(src, msg).Data()
	if env.Payload != "hello" {
		t.Fatalf("payload = %v, want hello", env.Payload)
	}
	if env.Attributes != nil {
		t.Fatalf("attributes must be nil when IncludeAttributes=false, got %v", env.Attributes)
	}
}

func TestMessage_Data_WithIncludeAttributes(t *testing.T) {
	src := newMinimalSource(true)
	msg := &sqstypes.Message{
		Body:      aws.String("world"),
		MessageId: aws.String("id-2"),
		Attributes: map[string]string{
			"SentTimestamp": "999",
		},
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"Key": {DataType: aws.String("String"), StringValue: aws.String("val")},
		},
	}
	env := newTestMessage(src, msg).Data()
	if env.Payload != "world" {
		t.Fatalf("payload = %v, want world", env.Payload)
	}
	if env.Attributes == nil {
		t.Fatal("attributes must not be nil when IncludeAttributes=true")
	}
	if env.Attributes["SentTimestamp"] != "999" {
		t.Fatalf("SentTimestamp = %q, want 999", env.Attributes["SentTimestamp"])
	}
	if env.Attributes["Key"] != "val" {
		t.Fatalf("Key = %q, want val", env.Attributes["Key"])
	}
	if env.Attributes["MessageId"] != "id-2" {
		t.Fatalf("MessageId = %q, want id-2", env.Attributes["MessageId"])
	}
}

func TestMessage_Data_WithIncludeAttributes_EmptyMessage(t *testing.T) {
	src := newMinimalSource(true)
	msg := &sqstypes.Message{Body: aws.String("empty")}
	env := newTestMessage(src, msg).Data()
	if env.Payload != "empty" {
		t.Fatalf("payload = %v, want empty", env.Payload)
	}
	// No attributes, no MessageId → extractSQSAttributes returns nil.
	if env.Attributes != nil {
		t.Fatalf("attributes must be nil for empty message, got %v", env.Attributes)
	}
}

// --- message.EstimatedSizeBytes ---

func TestMessage_EstimatedSizeBytes(t *testing.T) {
	src := newMinimalSource(false)
	msg := &sqstypes.Message{Body: aws.String("hello")}
	m := newTestMessage(src, msg)
	n, ok := m.EstimatedSizeBytes()
	if !ok {
		t.Fatal("ok must be true")
	}
	if n != 5 {
		t.Fatalf("size = %d, want 5", n)
	}
}

func TestMessage_EstimatedSizeBytes_EmptyBody(t *testing.T) {
	src := newMinimalSource(false)
	msg := &sqstypes.Message{} // nil Body → aws.ToString returns ""
	m := newTestMessage(src, msg)
	n, ok := m.EstimatedSizeBytes()
	if !ok {
		t.Fatal("ok must be true")
	}
	if n != 0 {
		t.Fatalf("size = %d, want 0", n)
	}
}

// --- message.AckHandle ---

func TestMessage_AckHandle_BothFieldsSet(t *testing.T) {
	src := newMinimalSource(false)
	msg := &sqstypes.Message{
		MessageId:     aws.String("msg-id"),
		ReceiptHandle: aws.String("rh-123"),
	}
	h := newTestMessage(src, msg).AckHandle()
	if h.id != "msg-id" {
		t.Fatalf("id = %q, want msg-id", h.id)
	}
	if h.receiptHandle != "rh-123" {
		t.Fatalf("receiptHandle = %q, want rh-123", h.receiptHandle)
	}
}

func TestMessage_AckHandle_NilFields(t *testing.T) {
	src := newMinimalSource(false)
	msg := &sqstypes.Message{} // nil MessageId and ReceiptHandle
	h := newTestMessage(src, msg).AckHandle()
	if h.id != "" || h.receiptHandle != "" {
		t.Fatalf("expected empty ackHandle, got %+v", h)
	}
}

// --- message.AckMeta ---

func TestMessage_AckMeta_BothFieldsSet(t *testing.T) {
	src := newMinimalSource(false)
	msg := &sqstypes.Message{
		MessageId:     aws.String("m-99"),
		ReceiptHandle: aws.String("rh-99"),
	}
	meta, ok := newTestMessage(src, msg).AckMeta()
	if !ok {
		t.Fatal("ok must be true when ReceiptHandle is set")
	}
	if meta.ID != "m-99" {
		t.Fatalf("ID = %q, want m-99", meta.ID)
	}
	if meta.Handle != "rh-99" {
		t.Fatalf("Handle = %q, want rh-99", meta.Handle)
	}
}

// --- NewSourceSQSWithConfig with IncludeAttributes ---

// TestNewSourceSQSWithConfig_IncludeAttributes verifies that the constructor
// pre-allocates recvMsgAttrNames and recvAttrNames when IncludeAttributes=true.
func TestNewSourceSQSWithConfig_IncludeAttributes(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	cfg.IncludeAttributes = true
	cfg.Pollers = 1
	cfg.BufSize = 4

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Call the real constructor so the IncludeAttributes branch executes.
	src := NewSourceSQSWithConfig(ctx, testClientWrapper{api: f}, "https://sqs.us-east-1.amazonaws.com/123/test", cfg)
	defer src.Close()

	if len(src.recvMsgAttrNames) == 0 {
		t.Fatal("recvMsgAttrNames must be populated when IncludeAttributes=true")
	}
	if len(src.recvAttrNames) == 0 {
		t.Fatal("recvAttrNames must be populated when IncludeAttributes=true")
	}
	if src.recvMsgAttrNames[0] != "All" {
		t.Fatalf("recvMsgAttrNames[0] = %q, want All", src.recvMsgAttrNames[0])
	}
}

// TestNewSourceSQSWithConfig_NoIncludeAttributes verifies that the slices are
// nil (not allocated) when IncludeAttributes is false.
func TestNewSourceSQSWithConfig_NoIncludeAttributes(t *testing.T) {
	f := newFakeSQSAPI(1)
	cfg := DefaultSourceSQSConfig
	cfg.IncludeAttributes = false
	cfg.Pollers = 1
	cfg.BufSize = 4

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	src := NewSourceSQSWithConfig(ctx, testClientWrapper{api: f}, "https://sqs.us-east-1.amazonaws.com/123/test", cfg)
	defer src.Close()

	if src.recvMsgAttrNames != nil {
		t.Fatal("recvMsgAttrNames must be nil when IncludeAttributes=false")
	}
	if src.recvAttrNames != nil {
		t.Fatal("recvAttrNames must be nil when IncludeAttributes=false")
	}
}

// TestSourceSQSConfig_Validate_Panics exercises the panic paths in validate.
func TestSourceSQSConfig_Validate_Panics(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(c *SourceSQSConfig)
		wantMsg string
	}{
		{"negative wait time", func(c *SourceSQSConfig) { c.WaitTimeSeconds = -1 }, "wait time"},
		{"wait time > 20", func(c *SourceSQSConfig) { c.WaitTimeSeconds = 21 }, "wait time"},
		{"max messages 0", func(c *SourceSQSConfig) { c.MaxMessages = 0 }, "max messages"},
		{"max messages > 10", func(c *SourceSQSConfig) { c.MaxMessages = 11 }, "max messages"},
		{"negative visibility", func(c *SourceSQSConfig) { c.VisibilityTO = -1 }, "visibility timeout"},
		{"pollers 0", func(c *SourceSQSConfig) { c.Pollers = 0 }, "pollers"},
		{"bufsize 0", func(c *SourceSQSConfig) { c.BufSize = 0 }, "buffer size"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultSourceSQSConfig
			tc.mutate(&cfg)
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for %s", tc.name)
				}
			}()
			cfg.validate()
		})
	}
}

func TestMessage_AckMeta_NilFields_EmptyStrings(t *testing.T) {
	src := newMinimalSource(false)
	msg := &sqstypes.Message{} // no MessageId, no ReceiptHandle
	meta, ok := newTestMessage(src, msg).AckMeta()
	if !ok {
		t.Fatal("AckMeta must always return ok=true")
	}
	if meta.ID != "" || meta.Handle != "" {
		t.Fatalf("expected empty metadata, got %+v", meta)
	}
}
