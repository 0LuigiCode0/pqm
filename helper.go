package pqm

func (c *column) Get() *column { return c }

func (k *key) Get() *key { return k }

func equalsArray(from, to []string) bool {
	flag := false
	if len(from) == 0 && len(to) == 0 {
		flag = true
	}
loop:
	for _, f := range from {
		for _, t := range to {
			if f == t {
				flag = true
				continue loop
			}
		}
		flag = false
	}
	return flag
}
