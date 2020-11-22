package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

//普通连接
//声明一个全局的rdb变量
var rdb *redis.Client

//初始化连接
func initClient() (err error) {
	rdb = redis.NewClient(&redis.Options{ //已声明rdb
		Addr:     "localhost:6379",
		Password: "",  //no password set
		DB:       0,   //use default DB
		PoolSize: 100, //连接池大小，视情况而定
	})

	_, err = rdb.Ping().Result()
	if err != nil {
		return err
	}
	return nil
}

//连接Redis哨兵模式
func initClient2() (err error) {
	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    "master",
		SentinelAddrs: []string{"x.x.x.x:26379", "xx.xx.xx.xx:26379", "xxx.xxx.xxx.xxx:26379"},
	})
	_, err = rdb.Ping().Result()
	if err != nil {
		return err
	}
	return nil
}

//连接Redis集群
func initClient3() (err error) {
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":7000", ":7001", ":7002", ":7003", ":7004", ":7005"},
	})
	_, err = rdb.Ping().Result()
	if err != nil {
		return err
	}
	return nil
}

//set/get示例
func redisExample() {
	err := rdb.Set("score", 100, 0).Err()
	if err != nil {
		fmt.Printf("set score failed, err:%v\n", err)
		return
	}

	val, err := rdb.Get("score").Result()
	if err != nil {
		fmt.Printf("get score failed, err:%v\n", err)
		return
	}
	fmt.Println("score", val)

	val2, err := rdb.Get("name").Result()
	if err == redis.Nil {
		fmt.Println("name does not exist")
	} else if err != nil {
		fmt.Printf("get name failed, err:%v\n", err)
		return
	} else {
		fmt.Println("name", val2)
	}
}

//zset示例
func redisExample2() {
	zsetKey := "language_rank"
	languages := []redis.Z{
		{Score: 90.0, Member: "Golang"},
		{Score: 98.0, Member: "Java"},
		{Score: 95.0, Member: "Python"},
		{Score: 97.0, Member: "JavaScript"},
		{Score: 99.0, Member: "C/C++"},
	}
	//ZADD
	fmt.Println("======================ZADD")
	num, err := rdb.ZAdd(zsetKey, languages...).Result()
	if err != nil {
		fmt.Printf("zadd failed, err:%v\n", err)
		return
	}
	fmt.Printf("zadd %d succ.\n", num)

	//把Golang的分数加10
	fmt.Println("======================把Golang的分数加10")
	newScore, err := rdb.ZIncrBy(zsetKey, 10.0, "Golang").Result()
	if err != nil {
		fmt.Printf("zincrby failed, err:%v\n", err)
		return
	}
	fmt.Printf("Golang's score is %f now.\n", newScore)

	//取分数最高的3个
	fmt.Println("======================取分数最高的3个")
	ret, err := rdb.ZRevRangeWithScores(zsetKey, 0, 2).Result()
	if err != nil {
		fmt.Printf("zrevrange failed, err:%v\n", err)
		return
	}
	for _, z := range ret {
		fmt.Println(z.Member, z.Score)
	}

	//取95~100分的
	fmt.Println("======================取95~100分的")
	op := redis.ZRangeBy{
		Min: "95",
		Max: "100",
	}
	ret, err = rdb.ZRangeByScoreWithScores(zsetKey, op).Result()
	if err != nil {
		fmt.Printf("zrangebyscore failed, err:%v\n", err)
		return
	}
	for _, z := range ret {
		fmt.Println(z.Member, z.Score)
	}
}

func pipelineDemo1() {
	pipe := rdb.Pipeline()

	incr := pipe.Incr("pipeline_counter")
	pipe.Expire("pipeline_counter", time.Hour)

	_, err := pipe.Exec()
	fmt.Println(incr.Val(), err)
	/*
		上面的代码相当于将以下两个命令一次发给redis server端执行，与不使用Pipeline相比能减少一次RTT。
		INCR pipeline_counter
		EXPIRE pipeline_counts 3600
	*/
}

func pipelinedDemo1() {
	var incr *redis.IntCmd
	_, err := rdb.Pipelined(func(pipe redis.Pipeliner) error {
		incr = pipe.Incr("pipelined_counter")
		pipe.Expire("pipelined_counter", time.Hour)
		return nil
	})
	fmt.Println(incr.Val(), err)

}

//事务
func txPipelineDemo1() {
	pipe := rdb.TxPipeline()

	incr := pipe.Incr("tx_pipeline_counter")
	pipe.Expire("tx_pipeline_counter", time.Hour)

	_, err := pipe.Exec()
	fmt.Println(incr.Val(), err)
	/*
		上面代码相当于在一个RTT下执行了下面的redis命令：
		MULTI
		INCR pipeline_counter
		EXPIRE pipeline_counts 3600
		EXEC
	*/
}

//还有一个与上文类似的TxPipelined方法，使用方法如下：
func pipelineDemo2() {
	var incr *redis.IntCmd
	_, err := rdb.TxPipelined(func(pipe redis.Pipeliner) error {
		incr = pipe.Incr("tx_pipelined_counter")
		pipe.Expire("tx_pipelined_counter", time.Hour)
		return nil
	})
	fmt.Println(incr.Val(), err)

}

/*
Watch
在某些场景下，我们除了要使用MULTI/EXEC命令外，还需要配合使用WATCH命令。
在用户使用WATCH命令监视某个键之后，直到该用户执行EXEC命令的这段时间里，
如果有其他用户抢先对被监视的键进行了替换、更新、删除等操作，那么当用户尝试执行EXEC的时候，
事务将失败并返回一个错误，用户可以根据这个错误选择重试事务或者放弃事务。

Watch(fn func(*Tx) error, keys ...string) error
Watch方法接收一个函数和一个或多个key作为参数。基本使用示例如下：
*/

func watchDemo1() {
	//监视watch_count的值，并在值不变的前提下将其值+1
	key := "watch_count"
	err := rdb.Watch(func(tx *redis.Tx) error {
		n, err := tx.Get(key).Int()
		if err != nil && err != redis.Nil {
			return err
		}
		_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
			//业务逻辑
			time.Sleep(time.Second * 5)
			pipe.Set(key, n+1, 0)
			return nil
		})
		return err
	}, key)
	if err != nil {
		fmt.Printf("tx exec failed, err:%v\n", err)
	}
}

//最后看一个官方文档中使用GET和SET命令以事务方式递增Key的值的示例：
func watchDemo2() {
	const routineCount = 100

	increment := func(key string) error {
		txf := func(tx *redis.Tx) error {
			//获得当前值或零值
			n, err := tx.Get(key).Int()
			if err != nil && err != redis.Nil {
				return err
			}

			//实际操作（乐观锁定中的本地操作）
			n++

			//仅在监视的Key保持不变的情况下运行
			_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
				//pipe 处理错误情况
				pipe.Set(key, n, 0)
				return nil
			})
			return err
		}

		for retries := routineCount; retries > 0; retries-- {
			err := rdb.Watch(txf, key)
			if err != redis.TxFailedErr {
				return err
			}
			//乐观锁丢失
		}
		return errors.New("increment reached maximum number of retries")
	}

	var wg sync.WaitGroup
	wg.Add(routineCount)
	for i := 0; i < routineCount; i++ {
		go func() {
			defer wg.Done()

			if err := increment("counter3"); err != nil {
				fmt.Println("increment error:", err)
			}
		}()
	}
	wg.Wait()

	n, err := rdb.Get("counter3").Int()
	fmt.Println("ended with", n, err)
}
func main() {
	if err := initClient(); err != nil {
		fmt.Printf("init redis client failed,err:%v\n", err)
	}
	fmt.Println("connect redis success...")
	//程序退出时释放资源
	defer rdb.Close()

	//redisExample()
	//redisExample2()
	watchDemo1()
}
