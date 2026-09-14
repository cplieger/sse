// Package sse is a broadcast hub for Server-Sent Events whose clients resume
// from an exact position: every frame carries an "<epoch>:<offset>" id, every
// connection opens with a hello that says whether the presented cursor was
// honoured, and a hub refuses at construction any retention it could not keep.
//
// A Hub owns the ring and the subscriber set; Serve adapts one HTTP request
// into a subscriber. The client presents its last id as Last-Event-ID and the
// hello answers with the epoch, the ring's floor and head, the keepalive and a
// Verdict: Resumed is true only when the ring covers every missed frame inside
// the reply cap, and a fresh connection, a cursor below the floor, another
// epoch's cursor or a malformed one all yield false, so the client reconciles.
//
// Every timing constant both halves share is written once in timing.json and
// pinned by a test in each language; the TypeScript client is published as
// @cplieger/sse from web/ and documented in web/README.md.
package sse
