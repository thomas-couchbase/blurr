package workloads

import (
	"math/rand"
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
	new_doc := workload.GenerateValue("000000000020", OVERHEAD)

	expected_doc := map[string]interface{}{
		"name": map[string]interface{}{
			"f": map[string]interface{}{
				"f": map[string]interface{}{
					"f": "ecdb3e e921c9",
				},
			},
		},
		"email": map[string]interface{}{
			"f": map[string]interface{}{
				"f": "3d13c6@a2d1f3.com",
			},
		},
		"street": map[string]interface{}{
			"f": map[string]interface{}{
				"f": "400f1d0a",
			},
		},
		"city": map[string]interface{}{
			"f": map[string]interface{}{
				"f": "90ac48",
			},
		},
		"county": map[string]interface{}{
			"f": map[string]interface{}{
				"f": "40efd6",
			},
		},
		"realm": map[string]interface{}{
			"f": "15e3f5",
		},
		"country": map[string]interface{}{
			"f": "1811db",
		},
		"coins": map[string]interface{}{
			"f": 213.54,
		},
		"state": map[string]interface{}{
			"f": "WY",
		},
		"full_state": map[string]interface{}{
			"f": "Montana",
		},
		"category":     int16(1),
		"achievements": []int16{0, 135, 92},
		"gmtime":       []int16{1972, 3, 3, 0, 0, 0, 4, 63, 0},
		"year":         int16(1989),
		"body":         "",
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

func BenchmarkZipfSize(b *testing.B) {
	src := rand.NewSource(0)
	r := rand.New(src)
	z := rand.NewZipf(r, 1.1, 9.0, 1000)
	for i := 0; i < b.N; i++ {
		z.Uint64()
	}
}
