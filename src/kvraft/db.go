package kvraft

type Db interface {
	Get(key string) (string, Err)
	Put(string, string) Err
	Append(string, string) Err
}

type MemoryDb struct {
	kv map[string]string
}

func NewMemoryDb() *MemoryDb {
	return &MemoryDb{make(map[string]string)}
}

func (m *MemoryDb) Get(key string) (string, Err) {
	if value, exist := m.kv[key]; exist {
		return value, OK
	}
	return "", ErrNoKey
}

func (m *MemoryDb) Put(key, val string) (string, Err) {
	m.kv[key] = val
	return "", OK
}

func (m *MemoryDb) Append(key, arg string) (string, Err) {
	m.kv[key] += arg
	return "", OK
}
