package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/joho/godotenv"
	"github.com/leonm1/flightsense-go/cache"
	"github.com/leonm1/flightsense-go/flight"
	"github.com/leonm1/flightsense-go/weather"
	csvmap "github.com/recursionpharma/go-csv-map"
)

const (
	workerCount = 32
)

func main() {
	// Create log file and direct output to file and console
	logFile, err := os.Create("log.txt")
	if err != nil {
		log.Fatal(err)
	}
	logW := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(logW)

	// Load files
	files, filenames, outPath := parseArguments()

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

	for i, in := range *files {
		log.Printf("Processing %s to %s", in, *outPath+(*filenames)[i])
		readFile(in, *outPath+(*filenames)[i])
	}

}

// readFile is the main function which executes the reading of a csv file from
//
func readFile(infilename string, outfilename string) {
	wg := new(sync.WaitGroup)
	workerWG := new(sync.WaitGroup)
	printc := make(chan *[]string, workerCount)
	jobs := make(chan *flight.Flight, workerCount)
	rowc := make(chan map[string]string, workerCount)

	// Create CSV reader
	file, err := os.Open(infilename)
	if err != nil {
		log.Fatalf("Cannot open '%s': %s\n", infilename, err.Error())
	}
	defer file.Close()
	r := csvmap.NewReader(file)

	// Read header
	r.Columns, err = r.ReadHeader()
	if err != nil {
		log.Fatal("Could not read csv header:", err)
	}

	// Start writer thread
	go printer(printc, &outfilename, wg)

	// Start worker threads
	workerWG.Add(workerCount)
	for w := 0; w < workerCount; w++ {
		go parser(rowc, jobs, wg)
		go worker(jobs, printc, workerWG)
	}

	// Iterate through file
	for row, err := r.Read(); err == nil; row, err = r.Read() {
		wg.Add(1)
		rowc <- row
	}
	// Signal end of parser pool work
	close(rowc)

	// Wait for printer to finish
	wg.Wait()
}

// worker is a worker responsible for getting the weather information for the source and
// destination airports for each *Flight sent over the jobs channel
func worker(jobs chan *flight.Flight, printc chan *[]string, workerWG *sync.WaitGroup) {
	// Accept incoming flights to process
	for f := range jobs {
		// Get weather at origin airport at time of departure
		weatherOrigin, err := weather.Get(f.Origin, f.ScheduledDep)
		if err != nil {
			log.Fatalf("Could not get weather for %s on %s: %s", f.Origin.IATA, f.ScheduledDep.String(), err)
		}

		weatherDest, err := weather.Get(f.Destination, f.ScheduledDep)
		if err != nil {
			log.Fatalf("Could not get weather for %s on %s: %s", f.Destination.IATA, f.ScheduledDep.String(), err)
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

		printc <- f.toSlice()
	}

	// Wait for worker pool to finish
	workerWG.Done()
	workerWG.Wait()
	close(printc)
}

// printer is a worker which uses a buffered writer to write each struct to a csv file
// the function listens on printc for new jobs. Not concurrency safe.
func printer(printc chan *[]string, outname *string, wg *sync.WaitGroup) {
	var f flight.Flight

	// Create and open outfile
	outfile, err := os.Create(*outname)
	if err != nil {
		log.Fatalf("Cannot open '%s': %s\n", *outname, err.Error())
	}

	w := csv.NewWriter(bufio.NewWriter(outfile))
	defer func() {
		w.Flush()
		outfile.Close()
	}()

	// Writer header to file
	w.Write(f)

	// Pull Flight objects from chan and print to file
	for j := range printc {
		w.Write(*j)
		wg.Done()
	}
}

func parseArguments() (*[]string, *[]string, *string) {
	var (
		files     []string
		filenames []string
		outPath   string
	)

	// Parse command-line flags for input and output files
	inname := flag.String("in", "", "Optional: Input file name (Cycles through directory if ommitted)")
	infolder := flag.String("indir", "", "Directory of source data files")
	outFolder := flag.String("outdir", "", "Directory of destination data files")
	flag.Parse()

	if *inname == "" && *infolder == "" {
		log.Fatalf("Input arguments requrired!")
		os.Exit(1)
	}

	if strings.Contains(*inname, "/") && *infolder == "" {
		s := strings.Split(*inname, "/")

		// Set filename as inname
		*inname = s[len(s)-1]

		// Remove filename
		s = s[:len(s)-1]
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

	return &files, &filenames, &outPath
}
