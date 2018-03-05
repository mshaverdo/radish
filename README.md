# Radish

Radish is a golang redis-like storage.

**Key features:**

* multithreaded
* strings, dicts, lists support 
* per-key TTL
* HTTP API
* high-performance RESP protocol, compatible with existing redis clients
* write-ahead log + storage snapshot persistence 


## Components
- `radish-server` - The server
- `radish-benchmark-http ` - HTTP API benchmarking tool

## Getting Started

### Getting Radish

```
$ git clone https://github.com/mshaverdo/radish
```

### Building Radish 

To build everything just type:
```
$ make
```

To build and test:
```
$ make test
```

To build and perform all tests with race detection, including integration tests:
```
$ make full-test
```

## Running 
To get server built-in cli hint:
```
$ ./radish-server --help
```

Run server on port 6380 with RESP protocol, with persistence into current dir, 
taking a storage snapshot every 10 minutes and sync write-ahead log to disk once a second:
```
$ ./radish-server -h localhost -p 6380 -d ./ -m 600 -s 1
```

or just

```
$ ./radish-server
```

to run Radish with HTTP API, add `-http` option:
```
$ ./radish-server -http
```

## Benchmark 

Standard `redis-benchmark` tool may be used to benchmarking. Due to limited command set, it's recommended to run in with 
`-t SET,GET,LPUSH,LPOP,HSET,LRANGE_100,LRANGE_300,LRANGE_450,LRANGE_600` parameter. 

Results for `./radish-server -d ""`  on  Amazon t2.2xlarge:

```
ubuntu@ip-172-31-41-114:~$ redis-benchmark  -p 6380 -e -q   -P 512 -n 10000000 -r 1000000 -t SET,GET,LPUSH,LPOP,HSET,LRANGE_100
SET: 2180074.25 requests per second
GET: 2227171.50 requests per second
LPUSH: 688800.12 requests per second
LPOP: 810044.56 requests per second
HSET: 1362583.38 requests per second
LPUSH (needed to benchmark LRANGE): 671591.69 requests per second
LRANGE_100 (first 100 elements): 114689.42 requests per second
```

`radish-benchmark-http` allows to benchmark HTTP API.

Results for `./radish-server -d "" -http`  on  Amazon t2.2xlarge:


```
ubuntu@ip-172-31-41-114:~$ ./radish-benchmark-http 
SET: 25000/25000 success
GET: 25000/25000 success
LPUSH: 25000/25000 success
LPOP: 25000/25000 success
Total: 100000/100000, 1.598863625s, 62544 requests per second
```


## API
### RESP
First of all, Radish provides redis-compatible RESP API with pipeline support and limited command set 
via https://github.com/tidwall/resp library. 
RESP is a default mode and allows to get a maximum performance from Radish. 
It compatible with existing Redis clients with few limitations:

* limited command set. Full list of available commands see in **HTTP** section
* SET is only standard: SET <key> <value>. For set-and-expire, please, use SETEX
* TTL doesn't support milliseconds


### HTTP-API Go client
Radish server is shipped with with the go client library, `github.com/mshaverdo/radish-client`

**Key features:**

* inspired by go-redis
* concurrency-safe
* command-as-method: `client.Get(key)` for `/GET/key` command
* go-redis-like return values: `StringResult`, `StringSliceResult`, `IntResult`, etc

please find more examples in `github.com/mshaverdo/radish-client/example`

### HTTP
Radish has RESTless HTTP network API. Generally, a command looks like `/<CMD>/<KEY>/<PARAM>`. 
For example, `/HGET/<KEY>/<FIELD>` returns the value in the field \<FIELD\> of dict in \<KEY\>.
`Content-Type: multipart/form-data` is utilized for requests or responses with multiple data items in one request (`LPUSH`, `KEYS`, `LRANGE`, etc).

The command execution status is placed into the `X-Radish-Status` header. Possible statuses:
* `StatusOk`  - Command processed successfully
* `StatusError` - General error
* `StatusNotFound` - Key not found
* `StatusTypeMismatch` - Trying to perform command on inappropriate key type (eg. `GET` on list) 


**SET**

 Set a key to hold a string value.
 If key already holds a value, it is overwritten, regardless of its type.
 Any previous time to live associated with the key is discarded on successful SET operation.

```
$ curl -v "http://localhost:6380/SET/K_12" -d "val12"
*   Trying 127.0.0.1...
* Connected to localhost (127.0.0.1) port 6380 (#0)
> POST /SET/K_12 HTTP/1.1
> Host: localhost:6380
> User-Agent: curl/7.47.0
> Accept: */*
> Content-Length: 5
> Content-Type: application/x-www-form-urlencoded
> 
* upload completely sent off: 5 out of 5 bytes
< HTTP/1.1 200 OK
< X-Radish-Status: StatusOk
< Date: Wed, 21 Feb 2018 01:53:46 GMT
< Content-Length: 0
< Content-Type: text/plain; charset=utf-8
< 
* Connection #0 to host localhost left intact
```

**GET**

 Get the value of key. If the key does not exist the special value nil is returned.
 An error is returned if the value stored at key is not a string, because GET only handles string values.

```
$ curl -v "http://localhost:6380/GET/K_12"  
*   Trying 127.0.0.1...
* Connected to localhost (127.0.0.1) port 6380 (#0)
> GET /GET/K_12 HTTP/1.1
> Host: localhost:6380
> User-Agent: curl/7.47.0
> Accept: */*
> 
< HTTP/1.1 200 OK
< X-Radish-Status: StatusOk
< Date: Wed, 21 Feb 2018 01:54:48 GMT
< Content-Length: 5
< Content-Type: text/plain; charset=utf-8
< 
* Connection #0 to host localhost left intact
val12
```
```
$ curl -v "http://localhost:6380/GET/K_13"  
*   Trying 127.0.0.1...
* Connected to localhost (127.0.0.1) port 6380 (#0)
> GET /GET/K_13 HTTP/1.1
> Host: localhost:6380
> User-Agent: curl/7.47.0
> Accept: */*
> 
< HTTP/1.1 404 Not Found
< X-Radish-Status: StatusNotFound
< Date: Wed, 21 Feb 2018 01:55:55 GMT
< Content-Length: 46
< Content-Type: text/plain; charset=utf-8
< 
* Connection #0 to host localhost left intact
Error processing "GET": "core: item not found"
```

**KEYS**

 Keys returns all keys matching a glob pattern.
 Warning: consider KEYS as a command that should only be used in production environments with extreme care.
 It may ruin performance when it is executed against large databases.


```
$ curl -v  "http://localhost:6380/KEYS/K*"  
*   Trying 127.0.0.1...
* Connected to localhost (127.0.0.1) port 6380 (#0)
> GET /KEYS/K* HTTP/1.1
> Host: localhost:6380
> User-Agent: curl/7.47.0
> Accept: */*
> 
< HTTP/1.1 200 OK
< Content-Type: multipart/form-data; boundary=a562708900b3e3e511fffd14509f4a3cae057da7a598e0cee088d8612a14
< X-Radish-Status: StatusOk
< Date: Wed, 21 Feb 2018 01:57:00 GMT
< Content-Length: 360
< 
--a562708900b3e3e511fffd14509f4a3cae057da7a598e0cee088d8612a14
Content-Type: text/plain

K_12
--a562708900b3e3e511fffd14509f4a3cae057da7a598e0cee088d8612a14
Content-Type: text/plain

K_10
--a562708900b3e3e511fffd14509f4a3cae057da7a598e0cee088d8612a14
Content-Type: text/plain

K_11
--a562708900b3e3e511fffd14509f4a3cae057da7a598e0cee088d8612a14--
* Connection #0 to host localhost left intact
```

**Full list of supported commands:**

Strings:
*  `/KEYS/<GLOB_PATTERN%>` - Keys returns all keys matching glob pattern. Returns multipart/form-data result.
*  `/GET/<KEY>` - Get the value of key. If the key does not exist the special value nil is returned.
*  `/SET/<KEY>` - Set key to hold the string value. Payload content in POST body.
*  `/SETEX/<KEY>/<TTL_SECONDS>` - Set key to hold the string value and set key to timeout after a given number of seconds. Payload content in POST body.
*  `/DEL/<KEY>[/<KEY>...]` - Del Removes the specified keys, ignoring not existing and returns count of actually removed values.

Dicts:
*  `/HKEYS/<KEY>` - Returns all field names in the dict stored at key. Returns multipart/form-data result.
*  `/HGETALL/<KEY>`- DGetAll Returns all fields and values of the hash stored at key. Returns multipart/form-data result.
*  `/HGET/<KEY>/<FIELD>` - DGet Returns the value associated with field in the dict stored at key.
*  `/HSET/<KEY>/<FIELD>` - DSet Sets field in the hash stored at key to value.  Payload content in POST body.
*  `/HDEL/<KEY>/<FIELD>[/<FIELD>...]` - DDel Removes the specified fields from the hash stored at key.

Lists:
*  `/LLEN/<KEY>` - LLen Returns the length of the list stored at key.
*  `/LRANGE/<KEY>/<START>/<STOP>`  - LRange returns the specified elements of the list stored at key. Returns multipart/form-data result.
*  `/LINDEX/<KEY>/<INDEX>` - LIndex Returns the element at index index in the list stored at key.
*  `/LSET/<KEY>/<INDEX>` -  LSet Sets the list element at index to value. Payload content in POST body.
*  `/LPUSH/<KEY>/` - LPush Insert all the specified values at the head of the list stored at key.  multipart/form-data Payload content in POST body.
*  `/LPOP/<KEY>/` - LPop Removes and returns the first element of the list stored at key.

TTL:
*  `/TTL/<KEY>` - Ttl Returns the remaining time to live of a key that has a timeout.
*  `/EXPIRE/<KEY>/<TTL_SECONDS>` - Expire sets a timeout on key. After the timeout has expired, the key will automatically be deleted.
*  `/PERSIST/<KEY>` - Persist Removes the existing timeout on key.

