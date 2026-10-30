package conninfo

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNextCountsRequestsPerConnection(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rtt, seq, ok := Next(r)
		fmt.Fprintf(w, "%d %v %v", seq, ok, rtt > 0)
	}))
	srv.Config.ConnContext = ConnContext
	srv.StartTLS()
	defer srv.Close()

	c := srv.Client()
	for i, want := range []string{"1 true true", "2 true true"} {
		resp, err := c.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(b) != want {
			t.Fatalf("request %d: got %q, want %q", i+1, b, want)
		}
	}
}
