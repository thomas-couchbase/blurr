package workloads

import (
	"testing"
)

var defaultWorkload Workload

var config = Config{
	CreatePercentage: 20,
	ReadPercentage:   20,
	UpdatePercentage: 20,
	DeletePercentage: 20,
	QueryPercentage:  20,
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

func BenchmarkGenerateValue_1_2048(b *testing.B) {
	const indexableFields = 1
	const size = 2048

	defaultWorkload = &Default{Config: config}
	for i := 0; i < b.N; i++ {
		key := defaultWorkload.GenerateNewKey(int64(i + 1))
		defaultWorkload.GenerateValue(key, indexableFields, size)
	}
}

func BenchmarkGenerateValue_8_2048(b *testing.B) {
	const indexableFields = 1
	const size = 2048

	defaultWorkload = &Default{Config: config}
	for i := 0; i < b.N; i++ {
		key := defaultWorkload.GenerateNewKey(int64(i + 1))
		defaultWorkload.GenerateValue(key, indexableFields, size)
	}
}

func BenchmarkGenerateValue_1_256(b *testing.B) {
	const indexableFields = 1
	const size = 256

	defaultWorkload = &Default{Config: config}
	for i := 0; i < b.N; i++ {
		key := defaultWorkload.GenerateNewKey(int64(i + 1))
		defaultWorkload.GenerateValue(key, indexableFields, size)
	}
}

func BenchmarkGenerateQuery_8(b *testing.B) {
	const indexableFields = 8

	defaultWorkload = &Default{Config: config}
	defaultWorkload.SetImplementation(defaultWorkload)
	for i := 0; i < b.N; i++ {
		defaultWorkload.GenerateQuery(indexableFields, int64(i+1))
	}
}
