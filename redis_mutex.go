package redismutex

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// ErrLockFail 获取redis锁失败
var (
	ErrLockFail       = errors.New("redis lock fail")
	ErrNotObtained    = errors.New("redis lock: not obtained")
	scriptSHA1Delete  string
	scriptSHA1Refresh string
	client            Client
)

const deleteScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end
`

const refreshScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then 
	return redis.call("pexpire", KEYS[1], ARGV[2]) 
else 
	return 0 
end
`

// Client redis 客户端接口
type Client interface {
	SetNX(key, value string, expire time.Duration) (bool, error)
	ScriptLoad(script string) (string, error)
	EvalSha(sha1 string, keys []string, args ...interface{}) (interface{}, error)
}

// RedisMutex 利用redis来做简易的分布式锁
// 详细见 https://redis.io/topics/distlock
// 获取锁 SET resouce_name my_random_value NX PX 30000
// 释放锁 使用lua脚本
// if redis.call("get", KEYS[1]) == ARGV[1] then
// 	return redis.call("del", KEYS[1])
// else
// 	return 0
// end
type RedisMutex struct {
	locker sync.Mutex
	name   string
	value  string
	expire time.Duration
	delay  time.Duration
	retry  int
}

// Lock obtain locker
// 获取锁 SET resouce_name my_random_value NX PX 30000
func (m *RedisMutex) Lock() error {
	m.locker.Lock()
	defer m.locker.Unlock()
	for i := 0; i < m.retry; i++ {
		ok, err := client.SetNX(m.name, m.value, m.expire)
		if ok && err == nil {
			return nil
		}
		time.Sleep(m.delay)
	}
	return ErrLockFail
}

// Unlock release locker
// 释放锁 使用lua脚本
// if redis.call("get", KEYS[1]) == ARGV[1] then
// 	return redis.call("del", KEYS[1])
// else
// 	return 0
// end
func (m *RedisMutex) Unlock() error {
	m.locker.Lock()
	defer m.locker.Unlock()
	if len(scriptSHA1Delete) == 0 {
		//load delete lua script and redis will cache it
		sha1, err := m.loadScript(deleteScript)
		if err != nil {
			return err
		}
		scriptSHA1Delete = sha1
	}
	v, err := client.EvalSha(scriptSHA1Delete, []string{m.name}, m.value)
	if err != nil {
		//retry
		time.Sleep(10 * time.Millisecond)
		v, err = client.EvalSha(scriptSHA1Delete, []string{m.name}, m.value)
	}
	if err != nil {
		return err
	}
	//check return result
	cnt, ok := v.(int64)
	if ok && cnt == 1 {
		return nil
	}
	msg := fmt.Sprintf("release redis locker err, del return value: %v", v)
	return errors.New(msg)
}

// Refresh locker ttl
// 刷新ttl 使用lua脚本
// if redis.call("get", KEYS[1]) == ARGV[1] then
// return redis.call("pexpire", KEYS[1], ARGV[2])
// else
// return 0
// end
func (m *RedisMutex) Refresh(ttl time.Duration) error {
	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	m.locker.Lock()
	defer m.locker.Unlock()
	if len(scriptSHA1Refresh) == 0 {
		//load refresh lua script and redis will cache it
		sha1, err := m.loadScript(refreshScript)
		if err != nil {
			return err
		}
		scriptSHA1Refresh = sha1
	}
	v, err := client.EvalSha(scriptSHA1Refresh, []string{m.name}, m.value, ttlVal)
	if err != nil {
		//retry
		time.Sleep(10 * time.Millisecond)
		v, err = client.EvalSha(scriptSHA1Refresh, []string{m.name}, m.value, ttlVal)
	}
	if err != nil {
		return err
	}
	//check return result
	cnt, ok := v.(int64)
	if ok && cnt == 1 {
		return nil
	}
	return ErrNotObtained
}

func (m *RedisMutex) loadScript(script string) (string, error) {
	sha1, err := client.ScriptLoad(script)
	if err != nil {
		//retry
		time.Sleep(10 * time.Millisecond)
		return client.ScriptLoad(script)
	}
	return sha1, err
}

// RedisLock 获取一个锁，并调用加锁方法
func RedisLock(name string) (*RedisMutex, error) {
	m := &RedisMutex{
		name:   name,
		value:  randomString(32),
		expire: 8 * time.Second,
		retry:  32,
		delay:  500 * time.Millisecond,
	}
	return m, m.Lock()
}

// NewRedisMutex 获取一个新的redis锁对象
// name 要锁住的资源名称
// 过期时间设置说明
// expire 表示超时时间
// retry 表示获取锁的尝试次数
// delay 重试之间的时间间隔
// NOTE 设置时间的时候请确保 delay * retry  > expire
// 这样可以保证在重试获取锁期间，之前的锁无论是主动释放，还是过期释放，都会释放掉
// NOTE 另外expire时间请设置的稍微长一点，如果任务执行时间过长，超过了expire时间，那么这个锁可能已经过期被释放了
// 默认锁的过期时间 8秒， 重试32次，每次间隔 500毫秒，这样重试总时间是16s
// 这里的expire time只能预估，对执行很久的任务，请使用其他的方式，或者避免用锁
func NewRedisMutex(name string, expire time.Duration, retry int, delay time.Duration) *RedisMutex {
	return &RedisMutex{
		name:   name,
		value:  randomString(32),
		expire: expire,
		retry:  retry,
		delay:  delay,
	}
}

// InitRedisClient 初始化一个redis client
func InitRedisClient(c Client) {
	client = c
}

var src = rand.NewSource(time.Now().UnixNano())

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// RandomString 根据指定长度返回随机字符串
func randomString(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}
