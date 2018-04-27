// Package weather provides helper methods for flightsense-go to fetch weather data
// from dark sky and cache it
package weather

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/leonm1/airports-go"
	"github.com/leonm1/flightsense-go/cache"

	darksky "github.com/mlbright/darksky/v2"
)

const darkSkyURL string = "https://api.darksky.net/forecast/"

// Get fetches the weather data (either from cache or darksky) and returns a map[string]interface{} of the json values
func Get(a airports.Airport, t time.Time) (map[string]string, error) {
	var (
		ret     map[string]string
		rndTime = t.Round(time.Hour)
		hash    = fmt.Sprintf("%x", sha1.Sum([]byte(a.IATA+fmt.Sprint(rndTime.Unix()))))
	)

	// In case of cache hit
	if res, err := cachemap.Get(hash); err == nil {
		err = json.Unmarshal([]byte(res), &ret)
		if err != nil {
			log.Fatal(err)
		}
		return ret, nil
	}

	log.Printf("Weather data does not exist in cache: %s", hash)

	// Form request and get data from darksky
	f, err := darksky.Get(os.Getenv("DARK_SKY_API_KEY"), fmt.Sprint(a.Latitude), fmt.Sprint(a.Longitude), fmt.Sprint(rndTime.Unix()), darksky.US, darksky.English)
	if err != nil {
		log.Print(f)
		log.Fatalf("Error fetching weather data from darksky: %s", err)
	}

	err = cache(a.IATA, f.Hourly.Data)

	// Resolve naming
	enc, err := json.Marshal(f.Currently)
	if err != nil {
		log.Fatalf("Error parsing weather data: %s", err)
	}

	err = json.Unmarshal(enc, &ret)
	if err != nil {
		log.Fatalf("Error parsing weather data: %s", err)
	}

	return ret, nil
}

func cache(iata string, f []darksky.DataPoint) error {
	var err error

	for _, v := range f {
		hash := fmt.Sprintf("%x", sha1.Sum([]byte(iata+fmt.Sprint(v.Time))))

		data, err := json.Marshal(v)
		if err != nil {
			log.Printf("Error caching data: %s", err)
		}

		cachemap.Set(hash, string(data))
	}

	return err
}
