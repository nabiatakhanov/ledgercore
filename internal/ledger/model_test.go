package ledger

import (
	"math"
	"math/big"
	"math/rand/v2"
	"testing"
)

func TestMoney(t *testing.T) {
	for _, s := range []string{"", "0", "000", "-1", "+1", "1.0", " 1", "9223372036854775808"} {
		if _, err := ParseMoney(s); err == nil {
			t.Errorf("accepted %q", s)
		}
	}
	for _, s := range []string{"1", "00042", "9223372036854775807"} {
		if _, err := ParseMoney(s); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range [][2]int64{{math.MaxInt64, 1}, {math.MinInt64, -1}} {
		if _, err := Add(p[0], p[1]); err == nil {
			t.Fatal("overflow accepted")
		}
	}
	if n, err := Add(math.MinInt64, math.MaxInt64); err != nil || n != -1 {
		t.Fatal(n, err)
	}
	if _, _, err := Move(1, math.MaxInt64, 1, false); err == nil {
		t.Fatal("destination overflow")
	}
	if _, _, err := Move(math.MinInt64, 0, 1, true); err == nil {
		t.Fatal("system underflow")
	}
	if _, _, err := Move(0, 0, 1, false); err == nil {
		t.Fatal("overdraft")
	}
}
func TestCanonicalRequest(t *testing.T) {
	a := TransferRequest{FromAccountID: " FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF ", ToAccountID: SystemAccountID, AmountMinor: "0001", Currency: " rub ", Metadata: map[string]string{"b": "2", "a": "1"}}
	b, n, err := a.Normalize()
	if err != nil || n != 1 {
		t.Fatal(n, err)
	}
	a.FromAccountID = b.FromAccountID
	a.AmountMinor = "1"
	a.Currency = "RUB"
	if Fingerprint("transfer", "", a) != Fingerprint("transfer", "", b) {
		t.Fatal("noncanonical fingerprint")
	}
	a.Metadata["a"] = "changed"
	if Fingerprint("transfer", "", a) == Fingerprint("transfer", "", b) {
		t.Fatal("metadata excluded or aliasing")
	}
	if Fingerprint("reversal", "a", b) == Fingerprint("reversal", "b", b) {
		t.Fatal("original excluded")
	}
	for _, id := range []string{"x", "000000000000-0000-0000-000000000001", "00000000-0000-0000-0000-00000000000g"} {
		if _, err := NormalizeID(id); err == nil {
			t.Fatal(id)
		}
	}
	for _, k := range []string{"", "a b", "ключ", "\n"} {
		if ValidateKey(k) == nil {
			t.Fatal(k)
		}
	}
	if ValidateKey("demo:001") != nil {
		t.Fatal("valid key")
	}
	if ValidateMetadata(map[string]string{"": "x"}) == nil || ValidateName(" ") == nil {
		t.Fatal("validation")
	}
}
func TestSequentialTransitionModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 99))
	balances := []int64{1000, 1000, 1000, 1000}
	reference := []*big.Int{big.NewInt(1000), big.NewInt(1000), big.NewInt(1000), big.NewInt(1000)}
	for i := 0; i < 10000; i++ {
		a := rng.IntN(4)
		b := (a + 1 + rng.IntN(3)) % 4
		n := int64(1 + rng.IntN(2500))
		x, y, err := Move(balances[a], balances[b], Money(n), false)
		enough := reference[a].Cmp(big.NewInt(n)) >= 0
		if (err == nil) != enough {
			t.Fatalf("step %d: %v", i, err)
		}
		if err == nil {
			balances[a] = x
			balances[b] = y
			reference[a].Sub(reference[a], big.NewInt(n))
			reference[b].Add(reference[b], big.NewInt(n))
		}
		for j, v := range balances {
			if v < 0 || v != reference[j].Int64() {
				t.Fatalf("step %d account %d", i, j)
			}
		}
	}
}
