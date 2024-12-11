package kvraft

//interface就是定义三个接口，后面指定具体实现函数
type KVStateMachine interface {
	Get(key string) (string,Err)
	Put(key string, value string) Err
	Append(key, value string) Err
}

type KV_dataset struct {
	KV map[string]string
}

func(KVdataset *KV_dataset) Get (key string) (string, Err) {
	if value, ok := KVdataset.KV[key] ; ok {
		return value, Ok
	}
	return "", ErrNoKey
}
func(KVdataset *KV_dataset) Put (key string, value string) Err {
	KVdataset.KV[key] = value
	return Ok
}
func(KVdataset *KV_dataset) Append(key, value string) Err {
	KVdataset.KV[key] += value
	return Ok
}