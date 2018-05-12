package parse

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	airlines "github.com/leonm1/airlines-go"
	airports "github.com/leonm1/airports-go"
	"github.com/leonm1/flightsense-go/cache"
	"github.com/leonm1/flightsense-go/flight"
	"github.com/leonm1/flightsense-go/weather"
)

// Worker is a worker responsible for converting rows from CSV into native go structs
// after receiving a job on the rowc channel, it sends a *Flight to worker on the jobs channel
func Worker(rowCh chan map[string]string, printCh chan []string, c *cache.Cache, wg *sync.WaitGroup) {
	for r := range rowCh {
		f, err := parse(r)
		if err != nil {
			log.Print(err)
			wg.Done()
			continue
		}

		err = getWeatherData(f, c)
		if err != nil {
			log.Print(err)
			wg.Done()
			continue
		}

		printCh <- f.ToSlice()
	}
}

func parse(r map[string]string) (*flight.Flight, error) {
	var (
		err          error
		crsDepTime   string
		depTime      string
		depDelay     string
		weatherDelay string
		location     *time.Location
		f            flight.Flight
	)

	for k, v := range r {
		switch {
		case k == "FL_DATE":
			f.Date = v
		case k == "CARRIER":
			carrier, err := airlines.LookupIATA(v)
			if err != nil {
				return nil, err
			}
			f.Carrier = carrier
		case k == "ORIGIN":
			orig, err := airports.LookupIATA(v)
			if err != nil {
				return nil, err
			}
			f.Origin = orig

			// Set timezone
			location, err = time.LoadLocation(f.Origin.Tz)
			if err != nil {
				return nil, err
			}
		case k == "DEST":
			dest, err := airports.LookupIATA(v)
			if err != nil {
				return nil, err
			}
			f.Destination = dest
		case k == "CANCELLED":
			if v == "1.00" { // Flight was cancelled
				f.Cancelled = true
			} else {
				f.Cancelled = false
			}
		case k == "CRS_DEP_TIME":
			crsDepTime = v
		case k == "CANCELLATION_CODE":
			f.CancellationCode = v
		case k == "DEP_TIME":
			depTime = v
		case k == "DEP_DELAY":
			depDelay = v
		case k == "DIVERTED":
			if v == "1.00" {
				f.Diverted = true
			} else {
				f.Diverted = false
			}
		case k == "WEATHER_DELAY":
			weatherDelay = v
		default:
		}
	}

	if f.Cancelled == false {
		// Estimated departure time
		if crsDepTime == "2400" {
			crsDepTime = "2359"
		}
		f.ScheduledDep, err = time.Parse("15042006-01-02", crsDepTime+f.Date)
		if err != nil {
			return nil, err
		}
		f.ScheduledDep = f.ScheduledDep.In(location)

		// Actual Departure time
		if depTime == "2400" {
			depTime = "2359"
		}
		f.ActualDep, err = time.Parse("15042006-01-02", depTime+f.Date)
		if err != nil {
			return nil, err
		}
		f.ActualDep = f.ActualDep.In(location)

		// Delay (in minutes)
		if weatherDelay != "" {
			delay, err := strconv.ParseFloat(depDelay, 64)
			if err != nil {
				return nil, err
			}
			if delay < 0 {
				delay = 0
			}
			f.Delay = int(delay)
		} else {
			f.Delay = 0
		}
	}

	return &f, nil
}

func getWeatherData(f *flight.Flight, c *cache.Cache) error {
	// Get weather at origin airport at time of departure
	weatherOrigin, err := weather.Get(f.Origin, f.ScheduledDep, c)
	if err != nil {
		return fmt.Errorf("Could not get weather for %s on %s: %s", f.Origin.IATA, f.ScheduledDep.String(), err)
	}

	weatherDest, err := weather.Get(f.Destination, f.ScheduledDep, c)
	if err != nil {
		return fmt.Errorf("Could not get weather for %s on %s: %s", f.Destination.IATA, f.ScheduledDep.String(), err)
	}

	// Parse origin fields
	f.TempOrigin = weatherOrigin.Temperature
	if weatherOrigin.PrecipIntensity == 0 {
		f.PrecipTypeOrigin = "none"
		f.PrecipIntensityOrigin = 0
	} else {
		f.PrecipTypeOrigin = weatherOrigin.PrecipType
		f.PrecipIntensityOrigin = weatherOrigin.PrecipIntensity
	}

	// Parse destination fields
	f.TempDest = weatherDest.Temperature
	if weatherDest.PrecipIntensity == 0 {
		f.PrecipTypeDest = "none"
		f.PrecipIntensityDest = 0
	} else {
		f.PrecipTypeDest = weatherDest.PrecipType
		f.PrecipIntensityDest = weatherDest.PrecipIntensity
	}

	return nil
}
