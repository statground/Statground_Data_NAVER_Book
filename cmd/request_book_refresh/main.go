package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"statground_naver_book_go/internal/bookrefreshreceipt"
	"statground_naver_book_go/internal/ch"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/kakaostore"
	"statground_naver_book_go/internal/util"
)

type refreshStage struct {
	View, Target, GenerationColumn, PressureTarget string
}

var stages = []refreshStage{
	{"Data_Book_Service.mv_book_catalog_latest_refresh", "Data_Book_Service.v_book_catalog_latest_current", "updated_at", "replica:Data_Book_Service.book_catalog_latest_local"},
	{"webr_book.mv_naver_r_book_catalog_refresh", "webr_book.v_naver_r_book_catalog", "refresh_batch", "replica:webr_book.naver_r_book_catalog_local"},
	{"mirtype_book.mv_naver_language_book_catalog_refresh", "mirtype_book.v_naver_language_book_catalog", "refresh_batch", "replica:mirtype_book.naver_language_book_catalog_local"},
	{"Data_Book_Service.mv_book_bibliography_discovery_refresh", "Data_Book_Service.v_book_bibliography_discovery", "updated_at", "replica:Data_Book_Service.book_bibliography_discovery_local"},
}

type refreshClient interface {
	QueryJSONEachRowContext(context.Context, string) ([]map[string]any, error)
	ExecContext(context.Context, string) error
}
type pressureGate func(context.Context, refreshStage) error

func main() {
	if err := run(); err != nil {
		// Transport errors can include endpoint URLs. Emit only the closed
		// operation category; credentials and response bodies stay private.
		fmt.Fprintln(os.Stderr, "book native refresh blocked; publication remains unverified")
		os.Exit(1)
	}
}

func run() error {
	receipt, err := bookrefreshreceipt.Read(strings.TrimSpace(os.Getenv("KAKAO_COLLECTION_RECEIPT_FILE")))
	if err != nil {
		return err
	}
	seconds := envx.Int("BOOK_REFRESH_REQUEST_TIMEOUT_SECONDS", 900)
	if seconds < 1 || seconds > 1800 {
		return errors.New("invalid book native refresh timeout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(seconds)*time.Second)
	defer cancel()
	client, err := ch.NewFromEnv()
	if err != nil {
		return err
	}
	client.HTTPClient.Timeout = 60 * time.Second
	if err := client.ValidateDirectEndpointHostnameContext(ctx, envx.String("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME", "")); err != nil {
		return err
	}
	return requestRefreshes(ctx, client, receipt, runPressureGate, time.Second)
}

// This command requests existing DB-owned native jobs. It never creates a
// writer, changes a schedule/dependency, or invokes STOP/START/ALTER/CANCEL.
// Keeper/native MV scheduling remains the sole execution owner. The separate
// manual-maintenance command still requires its durable writer lease.
func requestRefreshes(ctx context.Context, client refreshClient, receipt bookrefreshreceipt.Receipt, gate pressureGate, poll time.Duration) error {
	if err := receipt.ValidateCompleted(); err != nil {
		return err
	}
	config := kakaostore.ConfigFromEnv()
	if _, _, err := kakaostore.ConfirmCollectionSource(ctx, client, config.RawTable, config.OutboxTable, receipt.RunUUID, receipt.Inserted, receipt.LatestInsertedMillis); err != nil {
		return err
	}
	required := max(receipt.StartedMillis, receipt.LatestInsertedMillis)
	for _, stage := range stages {
		state, err := readState(ctx, client, stage.View)
		if err != nil {
			return err
		}
		if err := healthyState(state); err != nil {
			return err
		}
		// An existing native run may have started before this collection. Wait
		// for it to finish before requesting a new run; that older completion
		// must never be reported as this collection's refreshed generation.
		if running(state) {
			if err := waitNative(ctx, client, stage.View); err != nil {
				return err
			}
			state, err = waitState(ctx, client, stage.View, 0, poll)
			if err != nil {
				return err
			}
		}
		if err := waitChainIdle(ctx, client, poll); err != nil {
			return err
		}
		state, err = readState(ctx, client, stage.View)
		if err != nil {
			return err
		}
		if gate == nil || gate(ctx, stage) != nil {
			return errors.New("book native refresh pressure gate blocked")
		}
		// Refresh metadata is second-resolution. Cross the source watermark's
		// next whole second on the server before issuing the native request.
		watermark := (required/1000 + 1) * 1000
		for state.Observed < watermark {
			if err := pause(ctx, poll); err != nil {
				return err
			}
			state, err = readState(ctx, client, stage.View)
			if err != nil || healthyState(state) != nil {
				return errors.New("book native refresh metadata unavailable")
			}
		}
		if state.Started < watermark || state.Success < state.Started {
			// At most one request per stage. Reuse a native generation that
			// already consumed the same-run start/source and previous stage.
			// A timeout is ambiguous and never triggers another refresh request.
			if err := waitChainIdle(ctx, client, poll); err != nil {
				return err
			}
			if err := client.ExecContext(ctx, "SYSTEM REFRESH VIEW "+stage.View); err != nil {
				return err
			}
			if err := waitNative(ctx, client, stage.View); err != nil {
				return err
			}
			state, err = waitState(ctx, client, stage.View, watermark, poll)
			if err != nil {
				return err
			}
		}
		rows, err := client.QueryJSONEachRowContext(ctx, "SELECT count() AS row_count, ifNull(toUnixTimestamp64Milli(toDateTime64(max("+stage.GenerationColumn+"),3,'UTC')),0) AS generation_epoch FROM "+stage.Target+" SETTINGS max_threads=1,max_execution_time=10")
		if err != nil || len(rows) != 1 || rows[0]["row_count"] == nil || rows[0]["generation_epoch"] == nil {
			return errors.New("book native refresh target readback unavailable")
		}
		rowCount, countOK := util.NonnegativeInteger(rows[0]["row_count"])
		generation, generationOK := util.NonnegativeInteger(rows[0]["generation_epoch"])
		if !countOK || !generationOK || generation > state.Observed || rowCount > 0 && generation == 0 {
			return errors.New("book native refresh target generation invalid")
		}
		fmt.Printf("book_native_refresh view=%s source_watermark_millis=%d refresh_started_millis=%d refresh_success_millis=%d target_rows=%d target_generation_millis=%d publication=unverified\n", stage.View, receipt.LatestInsertedMillis, state.Started, state.Success, rowCount, generation)
		required = state.Success
	}
	fmt.Println("book_native_refresh status=completed refresh_verified=true publication=unverified")
	return nil
}

type refreshState struct {
	Status                     string
	Observed, Started, Success int64
	Failed                     bool
}

func readState(ctx context.Context, client refreshClient, view string) (refreshState, error) {
	database, name, _ := strings.Cut(view, ".")
	query := "SELECT status,toUnixTimestamp64Milli(now64(3)) AS observed_epoch,toUnixTimestamp64Milli(toDateTime64(last_refresh_time,3,'UTC')) AS started_epoch,toUnixTimestamp64Milli(toDateTime64(last_success_time,3,'UTC')) AS success_epoch,notEmpty(exception) AS failed FROM system.view_refreshes WHERE database=" + util.SQLString(database) + " AND view=" + util.SQLString(name) + " LIMIT 2 SETTINGS max_threads=1,max_execution_time=5"
	rows, err := client.QueryJSONEachRowContext(ctx, query)
	if err != nil || len(rows) != 1 {
		return refreshState{}, errors.New("book native refresh coordinator unavailable")
	}
	row := rows[0]
	for _, field := range []string{"status", "observed_epoch", "started_epoch", "success_epoch", "failed"} {
		if row[field] == nil {
			return refreshState{}, errors.New("book native refresh metadata incomplete")
		}
	}
	observed, observedOK := util.NonnegativeInteger(row["observed_epoch"])
	started, startedOK := util.NonnegativeInteger(row["started_epoch"])
	success, successOK := util.NonnegativeInteger(row["success_epoch"])
	failed, failedOK := util.NonnegativeInteger(row["failed"])
	if !observedOK || !startedOK || !successOK || !failedOK || failed > 1 {
		return refreshState{}, errors.New("book native refresh metadata invalid")
	}
	return refreshState{Status: util.ToString(row["status"]), Observed: observed, Started: started, Success: success, Failed: failed != 0}, nil
}

func running(s refreshState) bool {
	return s.Status == "Running" || s.Status == "RunningOnAnotherReplica"
}
func healthyState(s refreshState) error {
	if s.Observed <= 0 || s.Started <= 0 || s.Failed || s.Started > s.Observed || s.Success > s.Observed {
		return errors.New("book native refresh metadata unhealthy")
	}
	if running(s) {
		if s.Started <= 0 || s.Observed-s.Started > int64(2*time.Hour/time.Millisecond) {
			return errors.New("book native refresh running generation overdue")
		}
		return nil
	}
	if s.Status != "Scheduled" && s.Status != "Scheduling" && s.Status != "WaitingForDependencies" {
		return errors.New("book native refresh schedule disabled or dependencies unavailable")
	}
	if s.Success <= 0 || s.Observed-s.Success > int64(13*time.Hour/time.Millisecond) {
		return errors.New("book native refresh has no fresh successful generation")
	}
	return nil
}
func waitChainIdle(ctx context.Context, client refreshClient, poll time.Duration) error {
	for {
		busy := false
		for _, stage := range stages {
			state, err := readState(ctx, client, stage.View)
			if err != nil {
				return err
			}
			if err := healthyState(state); err != nil {
				return err
			}
			if running(state) {
				busy = true
				if err := waitNative(ctx, client, stage.View); err != nil {
					return err
				}
			}
		}
		if !busy {
			return nil
		}
		if err := pause(ctx, poll); err != nil {
			return err
		}
	}
}
func waitNative(ctx context.Context, client refreshClient, view string) error {
	// WAIT is observation of the native queue, not a schedule mutation.
	return client.ExecContext(ctx, "SYSTEM WAIT VIEW "+view)
}
func waitState(ctx context.Context, client refreshClient, view string, watermark int64, poll time.Duration) (refreshState, error) {
	for {
		s, err := readState(ctx, client, view)
		if err != nil {
			return s, err
		}
		if err := healthyState(s); err != nil {
			return s, err
		}
		if !running(s) && s.Started >= watermark && s.Success >= s.Started {
			return s, nil
		}
		if err := pause(ctx, poll); err != nil {
			return s, err
		}
	}
}
func pause(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func runPressureGate(ctx context.Context, stage refreshStage) error {
	path := envx.String("BOOK_REFRESH_PRESSURE_GATE_HELPER", "scripts/clickhouse_pressure_gate.py")
	abs, err := filepath.Abs(path)
	if err != nil {
		return errors.New("book refresh pressure helper unavailable")
	}
	info, err := os.Lstat(abs)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 {
		return errors.New("book refresh pressure helper unsafe")
	}
	gateCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	command := exec.CommandContext(gateCtx, "python3", abs)
	for _, item := range os.Environ() {
		if !strings.HasPrefix(item, "CLICKHOUSE_PRESSURE_GATE_TARGETS=") {
			command.Env = append(command.Env, item)
		}
	}
	command.Env = append(command.Env, "CLICKHOUSE_PRESSURE_GATE_TARGETS="+stage.PressureTarget)
	if _, err := command.Output(); err != nil {
		return errors.New("book refresh pressure gate blocked")
	}
	return nil
}
