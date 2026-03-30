package main

import (
    "fmt"
    "math/rand"
    "os"
    "time"

    "statground_naver_book_go/internal/ch"
    "statground_naver_book_go/internal/collector"
    "statground_naver_book_go/internal/envx"
    "statground_naver_book_go/internal/naver"
    "statground_naver_book_go/internal/terms"
)

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}

func run() error {
    client, err := ch.NewFromEnv()
    if err != nil {
        return err
    }
    keys, err := naver.LoadAPIKeysFromEnv()
    if err != nil {
        return err
    }

    batchSize := envx.Int("BATCH_SIZE", 500)
    display := envx.Int("NAVER_DISPLAY", 100)
    reqsPerTerm := envx.Int("REQS_PER_TERM", 1)
    termLength := envx.Int("UNICODE_TERM_LENGTH", 1)

    c, err := collector.New(client, "raw_naver", keys, time.Now().UnixNano())
    if err != nil {
        return err
    }

    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    randomTerms := terms.RandomUnicodeTerms(batchSize, termLength, r)
    if len(randomTerms) == 0 {
        return fmt.Errorf("no unicode random-char terms generated")
    }

    for _, term := range randomTerms {
        if err := c.CollectTerm(term, "unicode_random_char", reqsPerTerm, display); err != nil {
            return err
        }
    }
    return nil
}
