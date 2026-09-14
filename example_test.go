package sse_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/cplieger/sse"
	"github.com/cplieger/sse/ssetest"
	"github.com/cplieger/webhttp/v3"
)

func ExampleNew() {
	hub, err := sse.New(
		sse.WithReplay(1024),
		sse.WithReplayTTL(10*time.Minute),
		sse.WithReplyMaxEvents(256),
	)
	if err != nil {
		panic(err)
	}

	offset, err := hub.Publish(sse.Event{Name: "notify", Data: []byte(`{"n":1}`)})
	fmt.Println(offset, err)

	pos := hub.Position()
	fmt.Println(pos.Floor, pos.Head)

	// A ring without an age bound is refused at construction.
	_, err = sse.New(sse.WithReplay(1024))
	fmt.Println(errors.Is(err, sse.ErrConfig))
	// Output:
	// 1 <nil>
	// 1 1
	// true
}

func ExampleHub_Serve() {
	hub := sse.MustNew(sse.WithReplay(64), sse.WithReplayTTL(10*time.Minute))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		hub.Serve(w, r,
			sse.OnConnect(func(w *sse.Writer, h sse.Hello) error {
				return w.Event("connected", fmt.Appendf(nil, `{"resumed":%t}`, h.Resumed))
			}),
			sse.WithClientTag(r.Header.Get("SSE-Client")),
		)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// A first connection presents no cursor: the hello is fresh.
	first, err := open(srv, "")
	if err != nil {
		panic(err)
	}
	frames, err := ssetest.ReadFrames(first.Body, 3)
	if err != nil {
		panic(err)
	}
	fmt.Println(frames[0].Event, frames[0].Data)
	fmt.Println(frames[1].Event, describe(frames[1].Data))
	fmt.Println(frames[2].Event, frames[2].Data)

	if _, err := hub.Publish(sse.Event{Name: "notify", Data: []byte(`{"n":1}`)}); err != nil {
		panic(err)
	}
	live, err := ssetest.ReadFrames(first.Body, 1)
	if err != nil {
		panic(err)
	}
	cursor, err := sse.ParseCursor(live[0].ID)
	if err != nil {
		panic(err)
	}
	fmt.Println(live[0].Event, cursor.Offset, live[0].Data)
	first.Body.Close()

	// A frame published while the client is away is replayed on resume.
	if _, err := hub.Publish(sse.Event{Name: "notify", Data: []byte(`{"n":2}`)}); err != nil {
		panic(err)
	}
	second, err := open(srv, cursor.String())
	if err != nil {
		panic(err)
	}
	defer second.Body.Close()
	frames, err = ssetest.ReadFrames(second.Body, 4)
	if err != nil {
		panic(err)
	}
	fmt.Println(frames[1].Event, describe(frames[1].Data))
	fmt.Println(frames[2].Event, frames[2].ID == sse.Cursor{Epoch: cursor.Epoch, Offset: 2}.String(), frames[2].Data)
	fmt.Println(frames[3].Event, frames[3].Data)

	// Shutdown ends every stream with a reset frame naming the reason.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fmt.Println(hub.Shutdown(ctx))
	reset, err := ssetest.ReadFrames(second.Body, 1)
	if err != nil {
		panic(err)
	}
	fmt.Println(reset[0].Event, reset[0].Data)
	// Output:
	// retry 1500
	// sse:hello fresh resumed=false head=0
	// connected {"resumed":false}
	// notify 1 {"n":1}
	// sse:hello resumed resumed=true head=2
	// notify true {"n":2}
	// connected {"resumed":true}
	// <nil>
	// sse:reset {"reason":"shutdown"}
}

func ExampleHub_DigestHandler() {
	hub := sse.MustNew()
	current := map[sse.Subject]string{{Kind: "chat", Ref: "c1"}: "7"}
	resolve := func(_ context.Context, held []sse.Held) ([]sse.State, error) {
		states := make([]sse.State, 0, len(held))
		for _, h := range held {
			version, ok := current[h.Subject]
			if !ok {
				states = append(states, sse.State{Subject: h.Subject, Status: sse.StatusGone})
				continue
			}
			states = append(states, sse.State{Subject: h.Subject, Version: version})
		}
		return states, nil
	}
	srv := httptest.NewServer(webhttp.RouteTimeout(hub.DigestHandler(resolve), 10*time.Second, "digest timed out"))
	defer srv.Close()

	body := fmt.Sprintf(`{"epoch":%q,"subjects":[{"kind":"chat","ref":"c1","version":"5"},{"kind":"chat","ref":"c2","version":"1"}]}`, hub.Position().Epoch)
	resp, err := srv.Client().Post(srv.URL, "application/json", strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	var out struct {
		Changed []sse.Held `json:"changed"`
		Removed []struct {
			sse.Subject
			Reason sse.Status `json:"reason"`
		} `json:"removed"`
		MustRefetch bool `json:"must_refetch"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		panic(err)
	}
	fmt.Println(resp.StatusCode, out.MustRefetch)
	fmt.Println(out.Changed)
	fmt.Println(out.Removed)
	// Output:
	// 200 false
	// [{{chat c1} 7}]
	// [{{chat c2} gone}]
}

func open(srv *httptest.Server, cursor string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/api/events", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("SSE-Client", "tab-1")
	if cursor != "" {
		req.Header.Set("Last-Event-ID", cursor)
	}
	return srv.Client().Do(req)
}

func describe(hello string) string {
	var h sse.Hello
	if err := json.Unmarshal([]byte(hello), &h); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s resumed=%t head=%d", h.Verdict, h.Resumed, h.Head)
}
