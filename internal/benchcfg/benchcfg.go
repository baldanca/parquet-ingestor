package benchcfg

import "os"

const EnvProfile = "PARQUET_INGESTOR_BENCH_PROFILE"

func Profile() string {
	if p := os.Getenv(EnvProfile); p != "" {
		return p
	}
	if os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return "ci"
	}
	return "full"
}

func IsCI() bool { return Profile() == "ci" }

func Ints(full, ci []int) []int {
	if IsCI() {
		return ci
	}
	return full
}

func Int(full, ci int) int {
	if IsCI() {
		return ci
	}
	return full
}
