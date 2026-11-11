// register_book_source retries only retained source registration. It does not
// recollect a provider, refresh a native view, or publish directly to a broker.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"statground_naver_book_go/internal/bookpipeline"
)

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("register_book_source", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	receipt := flags.String("receipt", "", "retained completed receipt")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *receipt == "" {
		return fmt.Errorf("book source registration arguments refused")
	}
	mode, err := bookpipeline.Mode()
	if err != nil || mode != "broker" {
		return fmt.Errorf("book source registration requires broker mode")
	}
	p, err := bookpipeline.LoadProfile()
	if err != nil {
		return err
	}
	result, err := bookpipeline.Register(ctx, p, *receipt)
	if err != nil {
		return err
	}
	fmt.Printf("book_source_registration status=registered durable=true batch_id=%s event_id=%s\n", result.BatchID, result.EventID)
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if run(ctx, os.Args[1:]) != nil {
		fmt.Fprintln(os.Stderr, "book source registration pending; retained receipt and WAL require reconciliation")
		os.Exit(1)
	}
}
