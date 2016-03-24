// A shim based on libpcre, due to standard Go (1.6) regexp package leaking memory.

package regexp

import (
	"errors"
	"github.com/glenn-brown/golang-pkg-pcre/src/pkg/pcre"
	"sync"
)

type Regexp struct {
	str     string
	regexp  pcre.Regexp
	matcher *pcre.Matcher
	mutex   sync.Mutex
}

func MustCompile(expr string) *Regexp {
	return &Regexp{str: expr, regexp: pcre.MustCompile(expr, 0)}
}

func Compile(expr string) (*Regexp, error) {
	if r, err := pcre.Compile(expr, 0); err != nil {
		return nil, errors.New(err.String())

	} else {
		return &Regexp{str: expr, regexp: r}, nil
	}
}

func (self *Regexp) MatchString(s string) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.matcher == nil {
		self.matcher = self.regexp.MatcherString(s, 0)
		return self.matcher.Matches()

	} else {
		return self.matcher.MatchString(s, 0)
	}
}

func (self *Regexp) FindStringSubmatch(s string) []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.matcher == nil {
		self.matcher = self.regexp.MatcherString(s, 0)

		if !self.matcher.Matches() {
			return nil
		}

	} else if !self.matcher.MatchString(s, 0) {
		return nil
	}

	result := make([]string, self.matcher.Groups()+1)

	for i := 0; i < self.matcher.Groups()+1; i++ {
		result[i] = self.matcher.GroupString(i)
	}

	return result
}

func (self *Regexp) String() string {
	return self.str
}

func (self Regexp) ShimTag() {}
