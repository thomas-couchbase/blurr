package workloads

import (
	"reflect"
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

func TestN1QLDoc(t *testing.T) {
	workload := N1QL{Config: config}
	new_doc := workload.GenerateValue("000000000020", 0)

	expected_doc := map[string]interface{}{
		"category":     int16(1),
		"city":         "90ac48",
		"coins":        213.54,
		"country":      "1811db",
		"county":       "40efd6",
		"email":        "3d13c6@a2d1f3.com",
		"full_state":   "Montana",
		"name":         "ecdb3e e921c9",
		"realm":        "15e3f5",
		"state":        "WY",
		"street":       "400f1d0a",
		"year":         int16(1989),
		"achievements": []int16{0, 135, 92},
		"gmtime":       []int16{1972, 3, 3, 0, 0, 0, 4, 63, 0},
	}

	for k, v := range expected_doc {
		eq := reflect.DeepEqual(v, new_doc[k])
		if !eq {
			t.Errorf("%s: %T(%v) != %T(%v)", k, v, v, new_doc[k], new_doc[k])
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
