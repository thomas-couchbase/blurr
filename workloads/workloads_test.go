package workloads

import (
	"testing"
)

var defaultWorkload Workload

var config = Config{
	CreatePercentage: 25,
	ReadPercentage:   25,
	UpdatePercentage: 25,
	DeletePercentage: 25,
	Records:          1,
}

func TestExistingKey(t *testing.T) {
	workloads := []Workload{&Default{Config: config}, &HotSpot{Config: config}}
	for _, workload := range workloads {
		existing := workload.GenerateExistingKey(config.Records)
		for_removal := workload.GenerateKeyForRemoval()
		if existing != for_removal {
			t.Errorf("%s != %s", existing, for_removal)
		}
	}
}

func BenchmarkDefaultExistingKeyGen(b *testing.B) {
	defaultWorkload = &Default{Config: config}
	for i := 0; i < b.N; i++ {
		defaultWorkload.GenerateExistingKey(100000)
	}
}

func BenchmarkPrepareBatch(b *testing.B) {
	defaultWorkload = &Default{Config: config}
	for i := 0; i < b.N; i++ {
		defaultWorkload.PrepareBatch()
	}
}

func BenchmarkGenerateValue_2048(b *testing.B) {
	const size = 2048

	defaultWorkload = &Default{Config: config}
	for i := 0; i < b.N; i++ {
		key := defaultWorkload.GenerateNewKey(int64(i + 1))
		defaultWorkload.GenerateValue(key, size)
	}
}

func BenchmarkGenerateValue_256(b *testing.B) {
	const size = 256

	defaultWorkload = &Default{Config: config}
	for i := 0; i < b.N; i++ {
		key := defaultWorkload.GenerateNewKey(int64(i + 1))
		defaultWorkload.GenerateValue(key, size)
	}
}

func BenchmarkRandString_256(b *testing.B) {
	const size = 256

	defaultWorkload = &Default{Config: config}
	for i := 0; i < b.N; i++ {
		key := defaultWorkload.GenerateNewKey(int64(i + 1))
		RandString(key, size)
	}
}

func BenchmarkRandString_2048(b *testing.B) {
	const size = 2048

	defaultWorkload = &Default{Config: config}
	for i := 0; i < b.N; i++ {
		key := defaultWorkload.GenerateNewKey(int64(i + 1))
		RandString(key, size)
	}
}
