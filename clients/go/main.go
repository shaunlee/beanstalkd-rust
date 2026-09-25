// Command smoke is a real-client smoke test using github.com/beanstalkd/go-beanstalk.
//
// Usage: go run . HOST:PORT
//
// It runs a full protocol flow against a (fresh) beanstalkd-compatible
// server, asserting on every result, and prints a transcript to stdout in
// the same line formats as clients/python/smoke.py (see that file), so the
// same normalizer (clients/normalize.py) applies. go-beanstalk returns stats
// as maps, so stats fields are printed in sorted key order.
package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"sort"
	"time"

	beanstalk "github.com/beanstalkd/go-beanstalk"
)

const (
	tubeA = "smoke-go-a"
	tubeB = "smoke-go-b"
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

func dial(addr string) *beanstalk.Conn {
	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	must(err, "dial")
	// A hung server must fail the run, not hang it.
	must(nc.SetDeadline(time.Now().Add(60*time.Second)), "set deadline")
	return beanstalk.NewConn(nc)
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: smoke HOST:PORT")
		os.Exit(2)
	}
	addr := os.Args[1]
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
	out("DONE")
}
