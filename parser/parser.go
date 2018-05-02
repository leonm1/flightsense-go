package parser

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	airlines "github.com/leonm1/airlines-go"
	airports "github.com/leonm1/airports-go"
	"github.com/leonm1/flightsense-go/flight"
)

// Worker is a worker responsible for converting rows from CSV into native go structs
// after receiving a job on the rowc channel, it sends a *Flight to worker on the jobs channel
func Worker(rowc chan *[]string, jobs chan *flight.Flight, h *[]string, wg *sync.WaitGroup) {
	for r := range rowc {
		f, err := parse(r)
		if err != nil {
			log.Print(err)
		}
	}
}

func parse(r *map[string]string, wg *sync.WaitGroup) (*flight.Flight, error) {
	check := func(e error) {
		if e != nil {
			return nil, fmt.Errorf("Line skipped due to parsing error: ", r, err)
		}
	}

	var (
		date         string
		crsDepTime   string
		depTime      string
		depDelay     string
		diverted     string
		weatherDelay string
	)

	for k, v := range r {
		switch {
		case k == "FL_DATE":
			f.Date = v
		case k == "CARRIER":
			carrier, err := airlines.LookupIATA(values["CARRIER"])
			check(err)
			f.Carrier = carrier
		case k == "ORIGIN":
			orig, err := airports.LookupIATA(values["ORIGIN"])
			check(err)
			f.Origin = orig

			// Set timezone
			location, err := time.LoadLocation(f.Origin.Tz)
			check(err)
		case k == "DEST":
			dest, err := airports.LookupIATA(values["DEST"])
			check(err)
			f.Destination = dest
		case k == "CANCELLED":
			if v == 1 { // Flight was cancelled
				f.Cancelled = true
			} else {
				f.Cancelled = false
			}
		case k == "CRS_DEP_TIME":
			crsDepDelay = v
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
		// Actual Departure time
		if depTime == "2400" {
			depTime = "2359"
		}
		f.ActualDep, err = time.Parse("15042006-01-02", depTime+f.Date)
		f.ActualDep = f.ActualDep.In(location)
		check(err)

		// Delay (in minutes)
		if weatherDelay != "" {
			delay, err := strconv.ParseFloat(depDelay, 64)
			check(err)
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
