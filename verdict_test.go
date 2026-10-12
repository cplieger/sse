package sse

import "testing"

func TestResolve_replyCapBoundary(t *testing.T) {
	const head, replyCap = 10, 4
	tests := []struct {
		name        string
		since       uint64
		wantVerdict Verdict
		wantResumed bool
	}{
		{name: "ExactlyCapBehindResumes", since: head - replyCap, wantVerdict: VerdictResumed, wantResumed: true},
		{name: "OneUnderCapResumes", since: head - replyCap + 1, wantVerdict: VerdictResumed, wantResumed: true},
		{name: "OnePastCapIsGapBudget", since: head - replyCap - 1, wantVerdict: VerdictGapBudget},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cur := Cursor{Epoch: testEpoch, Offset: tc.since}
			verdict, resumed := resolve(cur, true, testEpoch, 1, head, head, replyCap)
			if verdict != tc.wantVerdict || resumed != tc.wantResumed {
				t.Errorf("resolve(since=%d, head=%d, replyCap=%d) = %s, %t, want %s, %t", tc.since, head, replyCap, verdict, resumed, tc.wantVerdict, tc.wantResumed)
			}
		})
	}
}
