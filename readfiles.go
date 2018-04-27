package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
)

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
