// Package cache provides a sync.Map implementation with compressed disk i/o
package cache

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

type Cache struct {
	data sync.Map
	name string
}

// New returns an initialized cache
func New() *Cache {
	c := new(Cache)
	c.name = "weather.cache"

	return c
}

// Load initializes the in-memory map with the information from the disk cache
func Load(filename string) (*Cache, error) {
	c := new(Cache)
	c.name = "weather.cache"

	// Open cache file
	f, err := os.OpenFile(c.name, os.O_CREATE|os.O_RDONLY, 0644)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	// Wrap file in gzip reader
	r, err := gzip.NewReader(f)
	defer r.Close()
	if err != nil {
		return nil, err
	}

	// Wrap gzip reader in scanner
	s := bufio.NewScanner(r)

	// Scan each line in the file
	for s.Scan() {
		line := s.Text()
		val := strings.Split(line, "_")

		// Load into map
		if len(val) == 2 {
			if _, loaded := c.data.LoadOrStore(val[0], val[1]); loaded {
				log.Printf("Duplicate value in cache for key '%s'", val[0])
			}
		}
	}

	return c, nil
}

// Set caches a value in the map and writes it to disk
func (c *Cache) Set(k string, v string) error {
	if _, loaded := c.data.LoadOrStore(k, v); !loaded {
		c.appendFile(k, v)
	}

	return nil
}

// Get returns a value from the map
func (c *Cache) Get(k string) (string, error) {
	if v, ok := c.data.Load(k); ok {
		return v.(string), nil
	}

	return "", fmt.Errorf("Key not found")
}

// Export writes a new cache file to disk with 'filename' as the file name
func (c *Cache) Export(filename string) error {
	c.name = filename

	f, err := os.OpenFile(c.name, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()
	if err != nil {
		return err
	}

	// Wrap file in gzip writer
	w := gzip.NewWriter(f)
	defer w.Close()

	c.data.Range(func(k interface{}, v interface{}) bool {
		_, err = fmt.Fprintf(w, "%s_%s\n", k.(string), v.(string))
		if err != nil {
			log.Fatalf("Error writing cache file: '%s'", err)
		}

		return true
	})

	return nil
}

func (c *Cache) appendFile(k string, v string) error {
	f, err := os.OpenFile(c.name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := gzip.NewWriter(f)
	defer w.Close()

	// Print key and value delimited by an underscore '_'
	fmt.Fprintf(w, "%s_%s\n", k, v)

	return err
}
