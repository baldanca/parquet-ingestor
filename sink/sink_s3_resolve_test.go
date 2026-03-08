package sink

import "testing"

func TestS3SinkResolvePath(t *testing.T) {
	s := &S3Sink{bucket: "my-bucket", prefix: "prefix"}
	got := s.ResolvePath("/a/b/file.parquet")
	want := "s3://my-bucket/prefix/a/b/file.parquet"
	if got != want {
		t.Fatalf("ResolvePath() = %q, want %q", got, want)
	}
}
