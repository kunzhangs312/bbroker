package main

import (
    "log"
    "github.com/streadway/amqp"
    "github.com/garyburd/redigo/redis"
    "time"
    "encoding/json"
    "bytes"
    "sync"
    "strings"
        "flag"
    "path/filepath"
    "os"
    "go.uber.org/zap"
    "fmt"
    "strconv"
)

var (
    RedisPool       *redis.Pool
    RC              redis.Conn
    RCMutex         sync.Mutex
    AMQPConn        *amqp.Connection
    AMQPChannel     *amqp.Channel
    Logger          *zap.Logger

    RedisServer    = flag.String("redisServer", "47.106.253.159:6379", "")
    RedisPassword  = flag.String("redisPassword", "sjdtwigkvsmdsjfkgiw23usfvmkj2", "")
    RabbitMQServer = flag.String("rabbitmqserver", "amqp://admin:aa0987aa1234@47.106.253.159:5672/", "")
)

const REDISDB                   = 3
const REDISHASH                 = "taskinfo"
const REDISTASKQUEUE            = "userOperateTasks"
const RABBITMQSENDSWITCH        = "BrokerSendSwitch"
//const RABBITMQRECVSWITCH        = "BrokerRecvSwitch"
const RABBITMQCALLBACKQUEUE     = "BrokerReadQueue"
const REBOOTTASKHANDLERTIMEOUT  = 10 * time.Minute
const OTHERTASKHANDLERTIMEOUT   = 5  * time.Minute

// 定义来自矿管的数据格式，将原封不动的下发给矿机
type Task struct {
    TaskId          string        `json:"taskid"`
    UserId         	int64         `json:"user_id"`
    Action          string        `json:"action"`
    Parameter       string        `json:"parameter"`
    Maclist         []string      `json:"maclist"`
}

// 矿机完成任务的时间
type MachineTime struct {
    Mac             string        `json:"mac"`
    StartTime       int64         `json:"start_time"`
    EndTime         int64         `json:"end_time"`
}

// 定义写入到Redis中的数据格式
type TaskRedisData struct {
    Task
    MachineTime     []MachineTime `json:"machine_time"`
    IsPublished     bool          `json:"is_pub"`
    MachineNum      int64         `json:"machine_num"`
    RespMac         []string      `json:"resp_mac"`
    RespNum         int64         `json:"resp_num"`
    CompletedMac    []string      `json:"completed_mac"`
    CompletedNum    int64         `json:"completed_num"`
    IsFinished      bool          `json:"is_finished"`
    SuccessMac      []string      `json:"success_mac"`       // 成功完成任务的MAC
    SuccessNum      int64         `json:"success_num"`       // 成功完成任务的MAC个数
    FailedMac       []string      `json:"failed_mac"`        // 完成失败任务的MAC
    FailedNum       int64         `json:"failed_num"`        // 完成失败任务的MAC个数
    StartTime       int64         `json:"start_time"`
    EndTime         int64         `json:"end_time"`
    IsAbort         bool          `json:"is_abort"`
    AbortType       string        `json:"abort_type"`
}


// 定义矿机执行操作的结果
type Result struct {
    FinishStatus    string      `json:"finish_status"`      // 任务完成的状态，success or failed
    FailedReason    string      `json:"failed_reason"`      // 任务执行失败的原因
    FinishTime      int64       `json:"finish_time"`        // 任务结束的时间
}

// 定义矿机反馈的数据格式
type MachineBackData struct {
    UserId          int64       `json:"user_id"`
    Action          string      `json:"action"`
    TaskId          string      `json:"taskid"`
    Maclist         []string    `json:"maclist"`
    RespType        string      `json:"resp_type"`
    Result
}


// 致命报错处理
func FatalOnError(err error, msg string) {
    if err != nil {
        message := fmt.Sprintf("%s: %v", msg, err)
        Logger.Fatal(message)
    }
}

// 常规报错处理
func PrintfOnError(err error, msg string) {
    if err != nil {
        message := fmt.Sprintf("%s: %v", msg, err)
        Logger.Warn(message)
    } else {
        Logger.Info(msg)
    }
}

// 将int64类型转换为[]byte类型
//func IntToByte(num int64) ([]byte, error) {
//    var buffer bytes.Buffer
//    err := binary.Write(&buffer, binary.BigEndian, num)
//    if err != nil {
//        return nil, err
//    }
//
//    return buffer.Bytes(), nil
//}

// 将bytes类型转换为string
func BytesToString(b *[]byte) *string {
    s := bytes.NewBuffer(*b)
    r := s.String()
    return &r
}

// 判断元素是否存在切片中
func IsSliceExist(s string, slice []string) bool {
    for _, value := range slice {
        if value == s {
            return true
        }
    }

    return false
}

//初始化一个redis pool
func newRedisPool(server, password string) *redis.Pool {
    return &redis.Pool{
        MaxIdle:     3,
        MaxActive:   5,
        IdleTimeout: 240 * time.Second,
        Dial: func() (redis.Conn, error) {
            c, err := redis.Dial("tcp", server)
            FatalOnError(err, "Failed to connect to Redis")

            if _, err := c.Do("AUTH", password); err != nil {
                c.Close()
                return nil, err
            }

            if _, err = c.Do("SELECT", REDISDB); err != nil {
                c.Close()
                return nil, err
            }

            return c, err
        },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            if time.Since(t) < time.Minute {
                return nil
            }
            _, err := c.Do("PING")
            return err
        },
    }
}


// 处理任务的协程，该协程收到任务后，为任务生成唯一的ID，并将该数据发送到RabbitMQ交换机中，
// 当消费者接收到来自RabbitMQ的数据后，比对自己的MAC地址是否在数据中，如果在则接收任务并处理，
// 该协程则需要读取对应的回调队列的数据。消费者会通过该队列将数据返回。
func TaskHandler(taskData []byte) {
    var task Task
    if err := json.Unmarshal(taskData, &task); err != nil {
        PrintfOnError(err, "JSON unmarshaling failed")
        return
    }

    taskId := task.TaskId

    // 打开Channel并将填充的数据发送到RabbitMQ中
    ch, err := AMQPConn.Channel()
    if err != nil {
       PrintfOnError(err, "Failed to open a channel")
       return
    }
    defer ch.Close()

    // 声明路由以及路由的类型
    err = ch.ExchangeDeclare(RABBITMQSENDSWITCH,
        "fanout",
        false,
        false,
        false,
        false,
        nil)
    if err != nil {
        PrintfOnError(err, "Failed to ExchangeDeclare")
        return
    }

    //callbackQueue := RABBITMQCALLBACKQUEUE + strings.ToUpper(taskId)
    err = ch.Publish(
       RABBITMQSENDSWITCH,
       "",
       false,
       false,
       amqp.Publishing{
           ContentType:"text/plain",                     // 文本格式
           //ReplyTo:callbackQueue,                      // 接收矿机应答的队列
           Body:taskData,                                // 内容
       })

    if err != nil {
       message := "Failed to publish the task to RabbitMQ: " + taskId
       PrintfOnError(err, message)
       return
    }

    //log.Println("Publish data into RabbitMQ BrokerSwitch switch")

    // 填充写入到Redis中的数据
    taskRedisData := TaskRedisData{
        Task: task,
        MachineTime: []MachineTime{},
        IsPublished:true,
        MachineNum:int64(len(task.Maclist)),
        RespMac:[]string{},
        RespNum:0,
        CompletedMac:[]string{},
        CompletedNum:0,
        IsFinished:false,
        StartTime:time.Now().Unix(),
        EndTime:0,
        IsAbort:false,
        AbortType:"initiative",
    }

    // 将填充的数据写入到Redis中
    jsonTaskRedisData, err := json.Marshal(taskRedisData)
    if err != nil {
        PrintfOnError(err, "jsonTaskRedisData marshaling failed")
        return
    }
    RCMutex.Lock()
    RC.Do("HSET", REDISHASH, taskId, string(jsonTaskRedisData))
    RCMutex.Unlock()


    // 将任务发送给矿机后，等待矿机完成任务并上报完成的结果

    // 声明一个队列
    callbackQueueName := RABBITMQCALLBACKQUEUE + strings.ToUpper(taskId)
    readQueue, err := ch.QueueDeclare(callbackQueueName,
        false,
        true,           // Delete after consumer cancels or disconnects,
                                   // 需要生产者和消费者同时设置才行，如果生产者不设置，而消费者设置，则会报错
        false,
        false,
        nil)
    if err != nil {
        PrintfOnError(err, "Failed to QueueDeclare")
        return
    }

    var tm *time.Timer
    if task.Action == "Restart" {
        tm = time.NewTimer(REBOOTTASKHANDLERTIMEOUT)
    } else {
        tm = time.NewTimer(OTHERTASKHANDLERTIMEOUT)
    }


    for {
        //msgs, err := ch.Consume(readQueue.Name,
        //    "",
        //    true,
        //    false,
        //    false,
        //    false,
        //    nil)
        //if err != nil {
        //    PrintfOnError(err, "Receive machine response failed")
        //    continue
        //}
        select {
        case <-tm.C:    // 超时检查
            {
                PrintfOnError(nil, "Task handler timeout, goroutine exit")
                taskRedisData.IsAbort = true
                taskRedisData.AbortType = "timeout"
                taskRedisData.EndTime = time.Now().Unix()

                // 将填充的数据写入到Redis中
                jsonTaskRedisData, err := json.Marshal(taskRedisData)
                if err != nil {
                    PrintfOnError(err, "jsonTaskRedisData marshaling failed")
                    return
                }
                RCMutex.Lock()
                RC.Do("HSET", REDISHASH, taskId, string(jsonTaskRedisData))
                RCMutex.Unlock()
                return
            }
        default:
            msg, ok, err := ch.Get(readQueue.Name, true)
            if err != nil {
                PrintfOnError(err, "Receive machine response failed")
                continue
            }

            if !ok {
                //PrintfOnError(nil, "Receive machine response not ok")
                time.Sleep(10 * time.Second)
                continue
            }

            //PrintfOnError(nil, string(msg.Body))
            
            msgStr := BytesToString(&(msg.Body))
            message := fmt.Sprintf("[y] receive a response: %s", *msgStr)
            Logger.Info(message)

            var machineData MachineBackData
            if err := json.Unmarshal(msg.Body, &machineData); err != nil {
                PrintfOnError(err, "Unmarshal machine back data failed")
                continue
            }

            if taskId != machineData.TaskId {
                PrintfOnError(nil, "taskid: " + taskId + " not match response taskid: " + machineData.TaskId)
                continue
            }

            mac := machineData.Maclist[0]

            if machineData.RespType == "confirm" {
                if IsSliceExist(mac, taskRedisData.RespMac) {
                    PrintfOnError(nil, "Response Mac has existed")
                    continue
                }

                taskRedisData.RespMac = append(taskRedisData.RespMac, mac)
                taskRedisData.RespNum = int64(len(taskRedisData.RespMac))

                // 将矿机开始执行任务的时间记录下来
                isExist := false
                for _, mt := range taskRedisData.MachineTime {
                    if mac == mt.Mac {
                        isExist = true
                        break
                    }
                }

                if !isExist {
                    machineTime := MachineTime{Mac:mac, StartTime:time.Now().Unix()}
                    taskRedisData.MachineTime = append(taskRedisData.MachineTime, machineTime)
                }

                // 将填充的数据写入到Redis中
                jsonTaskRedisData, err := json.Marshal(taskRedisData)
                if err != nil {
                    PrintfOnError(err, "jsonTaskRedisData marshaling failed")
                    continue
                }
                RCMutex.Lock()
                RC.Do("HSET", REDISHASH, taskId, string(jsonTaskRedisData))
                RCMutex.Unlock()
            } else if machineData.RespType == "completed" {
                if IsSliceExist(mac, taskRedisData.CompletedMac) {
                    PrintfOnError(nil, "Completed Mac has existed")
                    continue
                }

                taskRedisData.CompletedMac = append(taskRedisData.CompletedMac, mac)
                taskRedisData.CompletedNum = int64(len(taskRedisData.CompletedMac))

                // 将矿机完成执行任务的时间记录下来
                for idx, mt := range taskRedisData.MachineTime {
                    if mac == mt.Mac {
                        taskRedisData.MachineTime[idx].EndTime = time.Now().Unix()
                        break
                    }
                }

                if taskRedisData.CompletedNum == taskRedisData.MachineNum {
                    taskRedisData.IsFinished = true
                    taskRedisData.EndTime = time.Now().Unix()
                }

                if machineData.Result.FinishStatus == "success" {
                    if IsSliceExist(mac, taskRedisData.SuccessMac) {
                        taskRedisData.SuccessMac = append(taskRedisData.SuccessMac, mac)
                        taskRedisData.SuccessNum = int64(len(taskRedisData.SuccessMac))
                    }
                } else if machineData.Result.FinishStatus == "failed" {
                    if IsSliceExist(mac, taskRedisData.FailedMac) {
                        taskRedisData.FailedMac = append(taskRedisData.FailedMac, mac)
                        taskRedisData.FailedNum = int64(len(taskRedisData.FailedMac))
                    }
                }

                // 将填充的数据写入到Redis中
                jsonTaskRedisData, err := json.Marshal(taskRedisData)
                if err != nil {
                    PrintfOnError(err, "jsonTaskRedisData marshaling failed")
                    continue
                }
                RCMutex.Lock()
                RC.Do("HSET", REDISHASH, taskId, string(jsonTaskRedisData))
                RCMutex.Unlock()

                if taskRedisData.CompletedNum == taskRedisData.MachineNum {
                    PrintfOnError(nil, "userid: " + strconv.FormatInt(taskRedisData.UserId,10) +
                        " taskid: " + taskId + " has complete, goroutine exit")
                    return
                }
            } else {
                PrintfOnError(nil, "response type is wrong")
                continue
            }
        }
    }
}


func main() {
    dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
    if err != nil {
        log.Fatalf("Can't get program's absolute path: %v", err)
    }

    LogOutputPath := dir + "/" + "broker-output.log"
    LogErrorOutputPath := dir + "/" + "broker-error.log"
    zapConfig := zap.NewDevelopmentConfig()
    zapConfig.OutputPaths = []string{"stdout", LogOutputPath}
    zapConfig.ErrorOutputPaths = []string{"stderr", LogErrorOutputPath}
    Logger, err = zapConfig.Build()
    if err != nil {
       log.Fatalf("can't initialize zap logger: %v", err)
    }
    defer Logger.Sync()

    Logger.Info("blockos manager broker starting...")


    // 连接RabbitMQ和Redis
    //AMQPConn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
    AMQPConn, err = amqp.Dial(*RabbitMQServer)
    FatalOnError(err, "Failed to connect to RabbitMQ")
    defer AMQPConn.Close()

    AMQPChannel, err = AMQPConn.Channel()
    FatalOnError(err, "Failed to open a channel")
    defer AMQPChannel.Close()

    flag.Parse()

RedisReconnect:
    RedisPool = newRedisPool(*RedisServer, *RedisPassword)
    defer RedisPool.Close()

    RC = RedisPool.Get()        // 从Redis矿池中获取一个连接
    defer RC.Close()            // 用完后将连接放回连接池

    Logger.Info("start receive task from redis queue ...")

    for {
        // hset对redis进行写操作时，只能对一个hash表有一个写操作，不能同时多个写操作。否则会报
        // use of closed network connection错误。
        RCMutex.Lock()
        reply, err := RC.Do("BRPOP", REDISTASKQUEUE, 1)
        RCMutex.Unlock()
        if reply == nil {
            //PrintfOnError(err, "brpop redis hash failed, continue")
            continue
        }

        if err != nil {
            PrintfOnError(err, "brpop redis hash failed, reconnect redis")
            goto RedisReconnect
        }

        redisListRead, err := redis.ByteSlices(reply, err)
        if err != nil {
            PrintfOnError(err, "reply to byte slice failed, continue")
            continue
        }

        // 来自矿管JSON数据格式： {"taskid": "", "user_id": "", "action": "","parameter": "{}", "maclist": []}
        taskData := redisListRead[1]
        message := fmt.Sprintf("[x] receive new task: %s", string(taskData))
        Logger.Info(message)

        go TaskHandler(taskData)
    }
}
