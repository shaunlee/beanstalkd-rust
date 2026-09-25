// Command smoke is a real-client smoke test using github.com/beanstalkd/go-beanstalk.
//
// Usage: go run . [--leave-jobs | --after-restart] HOST:PORT
//
// It runs a full protocol flow against a (fresh) beanstalkd-compatible
// server, asserting on every result, and prints a transcript to stdout in
// the same line formats as clients/python/smoke.py (see that file), so the
// same normalizer (clients/normalize.py) applies. go-beanstalk returns stats
// as maps, so stats fields are printed in sorted key order.
//
// --leave-jobs and --after-restart drive the binlog restart test exactly
// like the Python client (see clients/python/smoke.py).
//
// TLS: when SMOKE_TLS_CA is set, every connection is a crypto/tls
// connection verifying the server against that CA bundle (server name: the
// host part of HOST:PORT); SMOKE_TLS_CERT / SMOKE_TLS_KEY add a client
// certificate (mTLS). The library is unmodified: beanstalk.NewConn accepts
// any io.ReadWriteCloser.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sort"
	"time"

	beanstalk "github.com/beanstalkd/go-beanstalk"
)

const (
	tubeA    = "smoke-go-a"
	tubeB    = "smoke-go-b"
	tubeKeep = "smoke-go-keep"
)

func out(format string, args ...any) { fmt.Printf(format+"\n", args...) }

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "ASSERTION FAILED: "+format+"\n", args...)
	os.Exit(1)
}

func must(err error, what string) {
	if err != nil {
		fail("%s: unexpected error: %v", what, err)
	}
}

func expect(label string, got, want any) {
	if !reflect.DeepEqual(got, want) {
		fail("%s: got %#v, want %#v", label, got, want)
	}
}

// errName maps a go-beanstalk error to a stable name for the transcript.
func errName(err error) string {
	switch {
	case err == nil:
		return "OK"
	case errors.Is(err, beanstalk.ErrNotFound):
		return "NOT_FOUND"
	case errors.Is(err, beanstalk.ErrTimeout):
		return "TIMED_OUT"
	case errors.Is(err, beanstalk.ErrNotIgnored):
		return "NOT_IGNORED"
	case errors.Is(err, beanstalk.ErrBuried):
		return "BURIED"
	case errors.Is(err, beanstalk.ErrDeadline):
		return "DEADLINE_SOON"
	default:
		return "OTHER(" + err.Error() + ")"
	}
}

// dump returns a printer for the (map, error) pair returned by the client's
// stats calls, so that it can be applied directly: dump(scope)(t.Stats()).
func dump(scope string) func(map[string]string, error) map[string]string {
	return func(m map[string]string, err error) map[string]string { return dumpMap(scope, m, err) }
}

func dumpMap(scope string, m map[string]string, err error) map[string]string {
	must(err, scope)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out("[%s] %s: %s", scope, k, m[k])
	}
	return m
}

// tlsConfig returns the client TLS configuration from the environment,
// or nil for plain TCP.
func tlsConfig(addr string) *tls.Config {
	caFile := os.Getenv("SMOKE_TLS_CA")
	if caFile == "" {
		return nil
	}
	pem, err := os.ReadFile(caFile)
	must(err, "read SMOKE_TLS_CA")
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		fail("no certificate in %s", caFile)
	}
	host, _, err := net.SplitHostPort(addr)
	must(err, "split address")
	cfg := &tls.Config{RootCAs: roots, ServerName: host}
	if certFile := os.Getenv("SMOKE_TLS_CERT"); certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, os.Getenv("SMOKE_TLS_KEY"))
		must(err, "load client certificate")
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg
}

func dial(addr string) *beanstalk.Conn {
	var nc net.Conn
	var err error
	if cfg := tlsConfig(addr); cfg != nil {
		nc, err = tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, cfg)
	} else {
		nc, err = net.DialTimeout("tcp", addr, 5*time.Second)
	}
	must(err, "dial")
	// A hung server must fail the run, not hang it.
	must(nc.SetDeadline(time.Now().Add(60*time.Second)), "set deadline")
	return beanstalk.NewConn(nc)
}

func main() {
	args := os.Args[1:]
	mode := ""
	if len(args) == 2 {
		mode, args = args[0], args[1:]
	}
	if len(args) != 1 || (mode != "" && mode != "--leave-jobs" && mode != "--after-restart") {
		fmt.Fprintln(os.Stderr, "usage: smoke [--leave-jobs | --after-restart] HOST:PORT")
		os.Exit(2)
	}
	addr := args[0]
	if mode == "--after-restart" {
		afterRestart(addr)
		return
	}
	prod := dial(addr)
	defer prod.Close()
	work := dial(addr)
	defer work.Close()

	ta := beanstalk.NewTube(prod, tubeA)
	tb := beanstalk.NewTube(prod, tubeB)
	ws := beanstalk.NewTubeSet(work, tubeA, tubeB)

	// Empty-queue errors.
	_, _, err := ws.Reserve(0)
	out("reserve-with-timeout 0 on empty: %s", errName(err))
	expect("reserve empty", errName(err), "TIMED_OUT")
	_, _, err = ta.PeekReady()
	out("peek-ready on empty: %s", errName(err))
	expect("peek-ready empty", errName(err), "NOT_FOUND")

	// Puts with priorities; the most urgent is reserved first.
	ids := map[string]uint64{}
	for _, p := range []struct {
		body string
		pri  uint32
	}{{"low", 2000}, {"urgent", 10}, {"mid", 500}} {
		id, err := ta.Put([]byte(p.body), p.pri, 0, time.Minute)
		must(err, "put")
		ids[p.body] = id
		out("put %s pri=%d: job=%d", p.body, p.pri, id)
	}
	id, err := tb.Put([]byte("b1"), 100, 0, time.Minute)
	must(err, "put b1")
	ids["b1"] = id
	out("put b1 into %s: job=%d", tubeB, id)
	id, err = ta.Put([]byte("later"), 1, 100*time.Second, time.Minute)
	must(err, "put later")
	ids["later"] = id
	out("put later delay=100: job=%d", id)

	tubes, err := prod.ListTubes()
	must(err, "list-tubes")
	sort.Strings(tubes)
	out("list-tubes: %v", tubes)
	expect("list-tubes", tubes, []string{"default", tubeA, tubeB})

	id, body, err := ta.PeekReady()
	must(err, "peek-ready")
	out("peek-ready (%s): job=%d body=%s", tubeA, id, body)
	expect("peek-ready", id, ids["urgent"])
	id, body, err = ta.PeekDelayed()
	must(err, "peek-delayed")
	out("peek-delayed: job=%d body=%s", id, body)
	expect("peek-delayed", id, ids["later"])
	body, err = prod.Peek(ids["mid"])
	must(err, "peek")
	out("peek mid: body=%s", body)
	_, err = prod.Peek(999999)
	out("peek missing: %s", errName(err))
	expect("peek missing", errName(err), "NOT_FOUND")
	dump("stats-tube " + tubeA)(ta.Stats())

	var order []string
	for i := 0; i < 4; i++ {
		id, body, err := ws.Reserve(time.Second)
		must(err, "reserve")
		order = append(order, string(body))
		out("reserve: job=%d body=%s", id, body)
		switch string(body) {
		case "mid":
			dump("stats-job mid")(work.StatsJob(id))
			must(work.Touch(id), "touch")
			out("touch job=%d: ok", id)
			must(work.Bury(id, 7), "bury")
			out("bury job=%d pri=7: ok", id)
		case "low":
			must(work.Release(id, 3000, 100*time.Second), "release")
			out("release job=%d delay=100: ok", id)
		default:
			must(work.Delete(id), "delete")
			out("delete job=%d: ok", id)
		}
	}
	expect("reserve order", order, []string{"urgent", "b1", "mid", "low"})
	err = work.Delete(ids["urgent"])
	out("delete already-deleted: %s", errName(err))
	expect("double delete", errName(err), "NOT_FOUND")

	id, _, err = ta.PeekBuried()
	must(err, "peek-buried")
	out("peek-buried: job=%d", id)
	expect("peek-buried", id, ids["mid"])
	dump("stats-tube " + tubeA)(ta.Stats())

	// kick: buried jobs first, then (when none are buried) delayed jobs.
	n, err := ta.Kick(10)
	must(err, "kick")
	out("kick 10: %d", n)
	expect("kick buried", n, 1)
	n, err = ta.Kick(10)
	must(err, "kick")
	out("kick 10 (delayed): %d", n)
	expect("kick delayed", n, 2)

	// kick-job and reserve-job.
	body, err = work.ReserveJob(ids["mid"])
	must(err, "reserve-job")
	out("reserve-job mid: body=%s", body)
	must(work.Bury(ids["mid"], 5), "bury")
	out("bury job=%d: ok", ids["mid"])
	must(prod.KickJob(ids["mid"]), "kick-job")
	out("kick-job job=%d: ok", ids["mid"])
	err = prod.KickJob(999999)
	out("kick-job missing: %s", errName(err))
	expect("kick-job missing", errName(err), "NOT_FOUND")

	for _, want := range []string{"later", "mid", "low"} {
		id, body, err := ws.Reserve(time.Second)
		must(err, "reserve")
		out("reserve: job=%d body=%s", id, body)
		expect("drain", string(body), want)
		must(work.Delete(id), "delete")
		out("delete job=%d: ok", id)
	}
	_, _, err = ws.Reserve(time.Second)
	out("reserve-with-timeout 1 on empty: %s", errName(err))
	expect("reserve timeout", errName(err), "TIMED_OUT")

	// pause-tube.
	id, err = tb.Put([]byte("paused"), 0, 0, time.Minute)
	must(err, "put")
	out("put paused into %s: job=%d", tubeB, id)
	must(tb.Pause(time.Second), "pause-tube")
	out("pause-tube %s 1: ok", tubeB)
	st := dump("stats-tube " + tubeB)(tb.Stats())
	expect("pause", st["pause"], "1")
	_, _, err = ws.Reserve(0)
	out("reserve-with-timeout 0 while paused: %s", errName(err))
	expect("paused reserve", errName(err), "TIMED_OUT")
	rid, _, err := ws.Reserve(3 * time.Second)
	must(err, "reserve after pause")
	out("reserve after pause expiry: job=%d", rid)
	expect("after pause", rid, id)
	must(work.Delete(rid), "delete")
	out("delete job=%d: ok", rid)

	st = dump("stats")(prod.Stats())
	for _, k := range []string{"current-jobs-ready", "current-jobs-reserved", "current-jobs-delayed", "current-jobs-buried"} {
		expect(k, st[k], "0")
	}
	expect("total-jobs", st["total-jobs"], "6")
	if mode == "--leave-jobs" {
		leaveJobsAndHold(addr, prod)
		return
	}
	out("DONE")
}

// leaveJobsAndHold leaves jobs in known journaled states, then holds two
// reservations until stdin is closed (the runner kills the server
// meanwhile). Mirrors leave_jobs_and_hold in clients/python/smoke.py.
func leaveJobsAndHold(addr string, prod *beanstalk.Conn) {
	tk := beanstalk.NewTube(prod, tubeKeep)
	hold := dial(addr)
	put := func(body string, pri uint32, delay time.Duration) uint64 {
		id, err := tk.Put([]byte(body), pri, delay, time.Minute)
		must(err, "put "+body)
		out("put %s pri=%d delay=%d: job=%d", body, pri, int(delay.Seconds()), id)
		return id
	}
	reserveJob := func(id uint64) {
		_, err := hold.ReserveJob(id)
		must(err, "reserve-job")
	}

	put("keep-ready", 100, 0)
	put("keep-delayed", 200, 3600*time.Second)
	// Put a before b but bury b first: live FIFO order is b, a; after a
	// restart buried jobs are replayed in order of their first record.
	a := put("keep-buried-a", 300, 0)
	b := put("keep-buried-b", 300, 0)
	for _, x := range []struct {
		id  uint64
		pri uint32
	}{{b, 41}, {a, 42}} {
		reserveJob(x.id)
		must(hold.Bury(x.id, x.pri), "bury")
		out("reserve-job + bury job=%d pri=%d: ok", x.id, x.pri)
	}
	id, body, err := tk.PeekBuried()
	must(err, "peek-buried")
	out("peek-buried: job=%d body=%s", id, body)
	expect("live buried fifo", id, b)
	id = put("keep-kicked", 400, 0)
	reserveJob(id)
	must(hold.Bury(id, 400), "bury")
	must(prod.KickJob(id), "kick-job")
	out("reserve-job + bury + kick-job job=%d: ok", id)
	id = put("keep-released", 500, 0)
	reserveJob(id)
	must(hold.Release(id, 501, 7200*time.Second), "release")
	out("reserve-job + release job=%d pri=501 delay=7200: ok", id)
	// Reserved at crash; the last journaled record is the put.
	id = put("keep-reserved", 600, 0)
	reserveJob(id)
	out("reserve-job job=%d (held): ok", id)
	// Reserved at crash; the last journaled record is a release with a 1s
	// delay, which will have expired by the time the server restarts.
	id = put("keep-reserved-expired", 700, 0)
	reserveJob(id)
	must(hold.Release(id, 701, time.Second), "release")
	reserveJob(id)
	out("reserve-job + release delay=1 + reserve-job job=%d (held): ok", id)
	// Deleted: leaves only a delete record, but its id still counts.
	id = put("keep-deleted", 800, 0)
	must(prod.Delete(id), "delete")
	out("delete job=%d: ok", id)
	dump("stats-tube " + tubeKeep)(tk.Stats())
	time.Sleep(1200 * time.Millisecond) // let the 1s release delay expire
	out("HOLDING")
	// Keep both connections (and the reservations) open until the runner has
	// killed the server and closes our stdin.
	_, _ = io.ReadAll(os.Stdin)
}

// afterRestart dumps and checks the state recovered from the binlog, then
// drains it. Mirrors after_restart in clients/python/smoke.py.
func afterRestart(addr string) {
	out("--- after restart ---")
	c := dial(addr)
	defer c.Close()
	tk := beanstalk.NewTube(c, tubeKeep)
	st := dump("stats")(c.Stats())
	for k, want := range map[string]string{
		"current-jobs-ready": "4", "current-jobs-reserved": "0",
		"current-jobs-delayed": "2", "current-jobs-buried": "2", "total-jobs": "0",
	} {
		expect(k, st[k], want)
	}
	tubes, err := c.ListTubes()
	must(err, "list-tubes")
	out("list-tubes: %v", tubes)
	sorted := append([]string(nil), tubes...)
	sort.Strings(sorted)
	expect("list-tubes", sorted, []string{"default", tubeKeep})
	dump("stats-tube " + tubeKeep)(tk.Stats())

	var maxID uint64
	for _, s := range []struct {
		state string
		peek  func() (uint64, []byte, error)
		want  []string
	}{
		{"ready", tk.PeekReady, []string{"keep-ready", "keep-kicked", "keep-reserved", "keep-reserved-expired"}},
		{"buried", tk.PeekBuried, []string{"keep-buried-a", "keep-buried-b"}},
		{"delayed", tk.PeekDelayed, []string{"keep-delayed", "keep-released"}},
	} {
		var got []string
		for {
			id, body, err := s.peek()
			if errors.Is(err, beanstalk.ErrNotFound) {
				out("peek-%s: NOT_FOUND", s.state)
				break
			}
			must(err, "peek-"+s.state)
			out("peek-%s: job=%d body=%s", s.state, id, body)
			dump("stats-job " + string(body))(c.StatsJob(id))
			must(c.Delete(id), "delete")
			out("delete job=%d: ok", id)
			got = append(got, string(body))
			if id > maxID {
				maxID = id
			}
		}
		expect("recovered "+s.state+" jobs", got, s.want)
	}

	// Ids continue after the highest id in any record (the deleted job's).
	id, err := tk.Put([]byte("after-restart"), 0, 0, time.Minute)
	must(err, "put")
	out("put after-restart: job=%d", id)
	out("next-id minus highest recovered id: %d", id-maxID)
	expect("next id", id-maxID, uint64(2))
	must(c.Delete(id), "delete")
	out("delete job=%d: ok", id)
	st = dump("stats")(c.Stats())
	expect("total-jobs", st["total-jobs"], "1")
	out("DONE")
}
