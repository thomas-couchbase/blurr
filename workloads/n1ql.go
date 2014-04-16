package workloads

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type N1QL struct {
	Config       Config
	DeletedItems int64
	Zipf         rand.Zipf
	Default
}

func (w *N1QL) GenerateNewKey(currentRecords int64) string {
	return fmt.Sprintf("%012d", currentRecords)
}

func (w *N1QL) GenerateExistingKey(currentRecords int64) string {
	var randRecord int64
	total_records := currentRecords - w.DeletedItems
	hot_records := total_records * w.Config.HotDataPercentage / 100
	cold_records := total_records - hot_records
	if rand.Intn(100) < w.Config.HotSpotAccessPercentage {
		randRecord = 1 + w.DeletedItems + cold_records + rand.Int63n(hot_records)
	} else {
		randRecord = 1 + w.DeletedItems + rand.Int63n(cold_records)
	}
	return fmt.Sprintf("%012d", randRecord)
}

func (w *N1QL) GenerateKeyForRemoval() string {
	w.DeletedItems++
	return fmt.Sprintf("%012d", w.DeletedItems)
}

func reverse(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func build_alphabet(key string) string {
	return Hash(key) + Hash(reverse(key))
}

func build_name(alphabet string) string {
	return fmt.Sprintf("%s %s", alphabet[:6], alphabet[6:12])
}

func build_email(alphabet string) string {
	return fmt.Sprintf("%s@%s.com", alphabet[12:18], alphabet[18:24])
}

func build_city(alphabet string) string {
	return alphabet[24:30]
}

func build_realm(alphabet string) string {
	return alphabet[30:36]
}

func build_country(alphabet string) string {
	return alphabet[42:48]
}

func build_county(alphabet string) string {
	return alphabet[48:54]
}

func build_street(alphabet string) string {
	return alphabet[54:62]
}

func build_coins(alphabet string) float64 {
	var coins, _ = strconv.ParseInt(alphabet[36:40], 16, 0)
	return math.Max(0.1, float64(coins)/100.0)
}

func build_category(alphabet string) int16 {
	var category, _ = strconv.ParseInt(string(alphabet[41]), 16, 0)
	return int16(category % 3)
}

func build_year(alphabet string) int16 {
	var year, _ = strconv.ParseInt(string(alphabet[62]), 32, 0)
	return int16(1985 + year)
}

func build_state(alphabet string) string {
	idx := strings.Index(alphabet, "7") % NUM_STATES
	if idx == -1 {
		idx = 56
	}
	return STATES[idx][0]
}

func build_full_state(alphabet string) string {
	idx := strings.Index(alphabet, "8") % NUM_STATES
	if idx == -1 {
		idx = 56
	}
	return STATES[idx][1]
}

func build_gmtime(alphabet string) []int16 {
	var id, _ = strconv.ParseInt(string(alphabet[63]), 16, 0)
	seconds := 396 * 24 * 3600 * (id % 12)
	d := time.Duration(seconds) * time.Second
	t := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC).Add(d)

	return []int16{
		int16(t.Year()),
		int16(t.Month()),
		int16(t.Day()),
		int16(t.Hour()),
		int16(t.Minute()),
		int16(t.Second()),
		int16(t.Weekday() - 1),
		int16(t.YearDay()),
		int16(0),
	}
}

func build_achievements(alphabet string) (achievements []int16) {
	achievement := int16(256)
	for i, char := range alphabet[42:58] {
		var id, _ = strconv.ParseInt(string(char), 16, 0)
		achievement = (achievement + int16(id)*int16(i)) % 512
		if achievement < 256 {
			achievements = append(achievements, achievement)
		}
	}
	return
}

var OVERHEAD = int(450)

func (w *N1QL) RandSize(size int) int {
	if size == OVERHEAD {
		return 0
	}
	if rand.Float32() < float32(0.995) { // Outliers
		normal := rand.NormFloat64()*0.17 + 1.0
		rand_size := int(float64(size-OVERHEAD) * normal)
		return rand_size
	} else {
		return size * int(1+w.Zipf.Uint64())
	}
}

func (w *N1QL) GenerateValue(key string, size int) map[string]interface{} {
	if size < OVERHEAD {
		log.Fatalf("Wrong workload configuration: minimal value size is %v", OVERHEAD)
	}

	alphabet := build_alphabet(key)

	return map[string]interface{}{
		"name": map[string]interface{}{
			"f": map[string]interface{}{
				"f": map[string]interface{}{
					"f": build_name(alphabet),
				},
			},
		},
		"email": map[string]interface{}{
			"f": map[string]interface{}{
				"f": build_email(alphabet),
			},
		},
		"street": map[string]interface{}{
			"f": map[string]interface{}{
				"f": build_street(alphabet),
			},
		},
		"city": map[string]interface{}{
			"f": map[string]interface{}{
				"f": build_city(alphabet),
			},
		},
		"county": map[string]interface{}{
			"f": map[string]interface{}{
				"f": build_county(alphabet),
			},
		},
		"realm": map[string]interface{}{
			"f": build_realm(alphabet),
		},
		"country": map[string]interface{}{
			"f": build_country(alphabet),
		},
		"coins": map[string]interface{}{
			"f": build_coins(alphabet),
		},
		"state": map[string]interface{}{
			"f": build_state(alphabet),
		},
		"full_state": map[string]interface{}{
			"f": build_full_state(alphabet),
		},
		"category":     build_category(alphabet),
		"achievements": build_achievements(alphabet),
		"gmtime":       build_gmtime(alphabet),
		"year":         build_year(alphabet),
		"body":         RandString(key, w.RandSize(size)),
	}
}

func (w *N1QL) GenerateQueryArgs(key string) []interface{} {
	alphabet := build_alphabet(key)
	view := w.Config.Views[rand.Intn(len(w.Config.Views))]

	if view == "id_by_city" {
		return []interface{}{
			view,                 // view name
			build_city(alphabet), // key
		}
	} else {
		log.Fatalf("Uknown view: %s", view)
		return nil
	}
}
