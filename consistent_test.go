package consistent

import "testing"

func TestAdd(t *testing.T) {
	c := New()

	c.Add("127.0.0.1:8000")
	if len(c.sortedSet) != replicationFactor {
		t.Fatal("vnodes number is incorrect")
	}
}

func TestGet(t *testing.T) {
	c := New()

	c.Add("127.0.0.1:8000")
	host, err := c.Get("127.0.0.1:8000")
	if err != nil {
		t.Fatal(err)
	}

	if host != "127.0.0.1:8000" {
		t.Fatal("returned host is not what expected")
	}
}

func TestRemove(t *testing.T) {
	c := New()

	c.Add("127.0.0.1:8000")
	c.Remove("127.0.0.1:8000")

	if len(c.sortedSet) != 0 && len(c.hosts) != 0 {
		t.Fatal(("remove is not working"))
	}

}

func TestHosts(t *testing.T) {
	hosts := []string{
		"127.0.0.1:8000",
		"92.0.0.1:8000",
	}

	c := New()
	for _, h := range hosts {
		c.Add(h)
	}

	addedHosts := c.Hosts()
	for _, h := range hosts {
		found := false
		for _, ah := range addedHosts {
			if h == ah {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("missing host", h)
		}
	}

}
