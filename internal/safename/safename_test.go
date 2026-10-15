package safename

import "testing"

func TestValidate(t *testing.T) {
	valid := []string{"task-1", "abc_123", "msg.42", "UPPER"}
	for _, name := range valid {
		if err := Validate(name); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", name, err)
		}
	}
	invalid := []string{"", ".", "..", "a/b", "a\\b", "a:b", "a*b", "a?b", `a"b`, "a<b", "a>b", "a|b", "a\x00b"}
	for _, name := range invalid {
		if err := Validate(name); err == nil {
			t.Errorf("Validate(%q) = nil, want error", name)
		}
	}
}
