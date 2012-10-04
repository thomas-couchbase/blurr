package workloads

import (
	"testing"
)

var testWorkload = DefaultWorkload{
	Config: Config{
		CreatePercentage: 10,
		ReadPercentage:   10,
		UpdatePercentage: 10,
		DeletePercentage: 10,
		QueryPercentage:  60,
	},
}

func BenchmarkBatching(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testWorkload.PrepareBatch()
		for j := 0; j < 100; j++ {
		}
	}
}

func BenchmarkStreaming(b *testing.B) {
	ch := testWorkload.Something()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			<-ch
		}
	}
}
