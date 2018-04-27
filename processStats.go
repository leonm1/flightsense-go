package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"github.com/leonm1/airlines-go"
	"github.com/leonm1/airports-go"
	"github.com/leonm1/flightsense-go/cache"
	"github.com/leonm1/flightsense-go/weather"
)

const (
	weatherURL       = "http://localhost"
	concurrencyLimit = 8
)

var header = []string{"absoluteTime",
	"year",
	"month",
	"day",
	"airline",
	"originAirport",
	"destAirport",
	"scheduledDeparture",
	"actualDeparture",
	"delay",
	"cancelled",
	"cancellationCode",
	"diverted",
	"tempOrigin",
	"precipTypeOrigin",
	"precipIntensityOrigin",
	"tempDest",
	"precipTypeDest",
	"precipIntensityDest",
}

type date time.Time
type numBool bool

// Flight includes data relating to weather conditions and general flight information
type Flight struct {
	Date                  string           `json:"fullDate" csv:"FL_DATE"`
	Carrier               airlines.Airline `json:"carrier" csv:"CARRIER"`
	Origin                airports.Airport `json:"origin" csv:"ORIGIN"`
	Destination           airports.Airport `json:"destination" csv:"DEST"`
	ScheduledDep          time.Time        `json:"scheduledDep" csv:"CRS_DEP_TIME"`
	ActualDep             time.Time        `json:"actualDep" csv:"DEP_TIME"`
	Delay                 int              `json:"delay" csv:"DEP_DELAY"`
	Cancelled             bool             `json:"cancelled" csv:"CANCELLED"`
	CancellationCode      string           `json:"cancellationCode" csv:"CANCELLATION_CODE"`
	Diverted              bool             `json:"diverted" csv:"DIVERTED"`
	DaylightSavings       string           `json:"dst" csv:"DST"`
	TempOrigin            float64          `json:"tempOrigin" csv:"TEMP_ORIG"`
	PrecipIntensityOrigin float64          `json:"originPrecipIntensity" csv:"PRECIP_ORIG"`
	PrecipTypeOrigin      string           `json:"originPrecipType" csv:"PRECIP_TYPE_ORIG"`
	TempDest              float64          `json:"destTemp" csv:"TEMP_DEST"`
	PrecipIntensityDest   float64          `json:"destPrecipIntensity" csv:"PRECIP_DEST"`
	PrecipTypeDest        string           `json:"destPrecipType" csv:"PRECIP_TYPE_DEST"`
}

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
	// Create log file and direct output to file and console
	logFile, err := os.Create("log.txt")
	if err != nil {
		log.Fatal(err)
	}
	logW := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(logW)

	// Parse command-line flags for input and output files
	inname := flag.String("in", "", "Optional: Input file name")
	infolder := flag.String("indir", "", "Directory of source data files; Overrides in flag")
	outFolder := flag.String("outdir", "", "Directory of destination data files")
	flag.Parse()

	var (
		files     []string
		filenames []string
		outPath   string
	)

	if *inname == "" && *infolder == "" {
		log.Fatalf("Input arguments requrired!")
	}

	if strings.Contains(*inname, "/") && *infolder == "" {
		s := strings.Split(*inname, "/")

		// Set filename as inname
		*inname = s[len(s)-1]

		// Remove filename
		s = s[:len(s)-1]
		//log.Printf("*inname is now '%s'", *inname)
		*infolder = strings.Join(s, "/")
		log.Printf("*infolder is now '%s'", *infolder)
	}

	if !strings.HasSuffix(*outFolder, "/") {
		if *outFolder == "" {
			outPath = "./"
		} else {
			outPath = *outFolder + "/"
		}
	}

	// Read all csv files in indir, skipping subdirectories
	filepath.Walk(*infolder, func(path string, f os.FileInfo, _ error) error {
		if f.IsDir() && path != *infolder {
			log.Printf("Skipping dir \"%s\"", f.Name())
			// Skip subdirectories
			return filepath.SkipDir
		} else if *inname != "" {
			if f.Name() == *inname {
				files = append(files, path)
				filenames = append(filenames, f.Name())
			}
		} else if filepath.Ext(path) == ".csv" {
			files = append(files, path)
			filenames = append(filenames, f.Name())
		}

		return nil
	})

	// Check to ensure input files exist
	for _, v := range files {
		if _, err := os.Stat(v); err != nil {
			if os.IsNotExist(err) {
				log.Fatalf("Error 404 - File not found: \"%s\".\nHere's the error: %s", v, err)
			}
		} else {
			log.Printf("Found file \"%s\"", v)
		}
	}

	// Check if outdir exists
	if _, err := os.Stat(outPath); err != nil {
		if os.IsNotExist(err) {
			os.Create(outPath)
			log.Printf("Output directory not found, created '%s'", outPath)
		} else {
			log.Fatal(err)
		}
	}

	// Load environment vars (DARK_SKY_API_KEY)
	err = godotenv.Load(".env")
	if err != nil {
		log.Fatal("Client secrets not found. Please configure dotenv")
	}

	// Load weather data cache
	err = cachemap.Load("cache.txt")
	if err != nil {
		log.Fatal(err)
	}

	for i, in := range files {
		log.Printf("Processing %s to %s", in, outPath+filenames[i])
		readFile(in, outPath+filenames[i])
	}

}

func readFile(infilename string, outfilename string) {
	var (
		wg sync.WaitGroup
		f  Flight
	)
	printc := make(chan []string)
	donec := make(chan bool)
	conc := make(chan bool, 1) // 1 is concurrency limit for http get requests to darkSky
	defer close(donec)

	// Create CSV reader
	infile, err := os.Open(infilename)
	if err != nil {
		log.Fatalf("Cannot open '%s': %s\n", infilename, err.Error())
	}
	defer infile.Close()
	r := csv.NewReader(infile)

	// Read header row
	header, err := r.Read()
	check(err)

	// Start writer thread
	go printer(donec, printc, outfilename, f)

	jobs := make(chan bool, concurrencyLimit)

	// Iterate through
	for row, err := r.Read(); err == nil; row, err = r.Read() {
		wg.Add(1)
		jobs <- true // Limit concurrency

		func(row []string) {
			defer wg.Done()

			f, err := parseRow(row, header)
			if err != nil {
				log.Printf("Error parsing flight data, flight skipped: %s", err)
			} else {
				parseWeatherData(&f, conc)
				printc <- *toSlice(f)
			}

			// Concurrency limit
			<-jobs
		}(row)
	}

	wg.Wait()
	close(printc)
	<-donec
}

func parseWeatherData(f *Flight, conc chan bool) {
	weatherOrigin, err := weather.Get(f.Origin, f.ScheduledDep, conc)
	if err != nil {
		if err != nil {
			log.Fatalf("Could not get weather for %s on %s: %s", f.Origin.IATA, f.ScheduledDep.String(), err)
		}
	}

	weatherDest, err := weather.Get(f.Destination, f.ScheduledDep, conc)
	if err != nil {
		if err != nil {
			log.Fatalf("Could not get weather for %s on %s: %s", f.Destination.IATA, f.ScheduledDep.String(), err)
		}
	}

	// Parse origin fields
	f.TempOrigin = weatherOrigin["temperature"].(float64)
	if _, ok := weatherOrigin["precipIntensity"]; !ok {
		f.PrecipTypeOrigin = "none"
		f.PrecipIntensityOrigin = 0
	} else {
		f.PrecipTypeOrigin = weatherOrigin["precipType"].(string)
		f.PrecipIntensityOrigin = weatherOrigin["precipIntensity"].(float64)
	}

	// Parse destination fields
	f.TempDest = weatherDest["temperature"].(float64)
	if _, ok := weatherDest["precipIntensity"]; !ok {
		f.PrecipTypeDest = "none"
		f.PrecipIntensityDest = 0
	} else {
		f.PrecipTypeDest = weatherDest["precipType"].(string)
		f.PrecipIntensityDest = weatherDest["precipIntensity"].(float64)
	}
}

func parseRow(r []string, h []string) (Flight, error) {
	var (
		f   Flight
		err error
	)
	values := make(map[string]string)

	// Initialize values into map
	for i, v := range h {
		values[v] = r[i]
	}

	// Date
	f.Date = values["FL_DATE"]

	// Carrier airline struct
	carrier, err := airlines.LookupIATA(values["CARRIER"])
	if err != nil {
		return f, fmt.Errorf("line: %s\n could not find airline '%s': %s", r, values["CARRIER"], err.Error())
	}
	f.Carrier = carrier

	// Origin Airport struct
	orig, err := airports.LookupIATA(values["ORIGIN"])
	if err != nil {
		return f, fmt.Errorf("line: %s\n could not find airport '%s': %s", r, values["ORIGIN"], err.Error())
	}
	f.Origin = orig
	location, err := time.LoadLocation(f.Origin.Tz)
	check(err)

	// Destination Airport struct
	dest, err := airports.LookupIATA(values["DEST"])
	if err != nil {
		return f, fmt.Errorf("line: %s\n could not find airport '%s': %s", r, values["DEST"], err.Error())
	}
	f.Destination = dest

	// Cancellation status
	if values["CANCELLED"] == "1.00" {
		f.Cancelled = true
	} else {
		f.Cancelled = false
	}

	// Scheduled Departure time
	f.ScheduledDep, err = time.Parse("15042006-01-02", values["CRS_DEP_TIME"]+values["FL_DATE"])
	f.ScheduledDep = f.ScheduledDep.In(location)
	if err != nil {
		return f, fmt.Errorf("line: %s\n could not parse time '%s': %s", r, values["CRS_DEP_TIME"], err.Error())
	}

	// Cancellation code
	f.CancellationCode = values["CANCELLATION_CODE"]

	if !f.Cancelled {
		// Actual Departure time
		if values["DEP_TIME"] == "2400" {
			values["DEP_TIME"] = "2359"
		}
		f.ActualDep, err = time.Parse("15042006-01-02", values["DEP_TIME"]+values["FL_DATE"])
		f.ActualDep = f.ActualDep.In(location)
		if err != nil {
			return f, fmt.Errorf("line: %s\n could not parse time '%s': %s", r, values["DEP_TIME"], err.Error())
		}

		// Delay (in minutes)
		if values["WEATHER_DELAY"] != "" {
			delay, err := strconv.ParseFloat(values["DEP_DELAY"], 64)
			if err != nil {
				return f, fmt.Errorf("line: %s\n could not parse delay '%s': %s", r, values["DEP_DELAY"], err.Error())
			}
			if delay < 0 {
				delay = 0
			}
			f.Delay = int(delay)
		} else {
			f.Delay = 0
		}

		// Flight diverted flag
		// Cancellation status
		if values["DIVERTED"] == "1.00" {
			f.Diverted = true
		} else {
			f.Diverted = false
		}
	}

	return f, nil
}

func printer(donec chan bool, printc chan []string, outname string, f Flight) {
	// Create and open outfile
	outfile, err := os.Create(outname)
	if err != nil {
		log.Fatalf("Cannot open '%s': %s\n", outname, err.Error())
	}
	w := csv.NewWriter(outfile)
	defer func() {
		w.Flush()
		outfile.Close()
	}()

	// Writer header to file
	w.Write(header)

	// Pull Flight objects from chan and print to file
	for {
		f, more := <-printc
		if more {
			w.Write(f)
		} else {
			// Signal end to main
			donec <- true
			return
		}
	}
}

func toSlice(f Flight) *[]string {
	ret := make([]string, 19)

	ret[0] = f.Date
	ret[1] = fmt.Sprint(f.ScheduledDep.Year())
	ret[2] = f.ScheduledDep.Month().String()
	ret[3] = fmt.Sprint(f.ScheduledDep.Day())
	ret[4] = f.Carrier.Name
	ret[5] = f.Origin.IATA
	ret[6] = f.Destination.IATA
	ret[7] = fmt.Sprintf("%02d%02d", f.ScheduledDep.Hour(), f.ScheduledDep.Minute())
	ret[8] = fmt.Sprintf("%02d%02d", f.ActualDep.Hour(), f.ActualDep.Minute())
	ret[9] = fmt.Sprint(f.Delay)
	ret[10] = strconv.FormatBool(f.Cancelled)
	ret[11] = f.CancellationCode
	ret[12] = strconv.FormatBool(f.Diverted)
	ret[13] = strconv.FormatFloat(f.TempOrigin, 'f', -1, 64)
	ret[14] = f.PrecipTypeOrigin
	ret[15] = strconv.FormatFloat(f.PrecipIntensityOrigin, 'f', -1, 64)
	ret[16] = strconv.FormatFloat(f.TempDest, 'f', -1, 64)
	ret[17] = f.PrecipTypeDest
	ret[18] = strconv.FormatFloat(f.PrecipIntensityDest, 'f', -1, 64)

	return &ret
}
