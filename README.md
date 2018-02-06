GET /KEYS
GET /GET/<KEY>
POST /SET/<KEY>
POST /DEL/<KEY>

GET /HKEYS/<KEY>
GET /HVALS/<KEY>
GET /HGET/<KEY>/<FIELD>
POST /HSET/<KEY>/<FIELD>
POST /HDEL/<KEY>/<FIELD>

GET /LLEN/<KEY>
GET /LRANGE/<KEY>/<START>/<STOP>
GET /LINDEX/<KEY>/<INDEX>
POST /LSET/<KEY>/<INDEX>
POST /LREM/<KEY>/<FIELD>

per-key ttl:
set HTTP header "X-Radish-TTL: <TTL IN SECONDS>"



#проверить, как в редис происходит удаление/создание списков
#пары значений не предоставлять, только списки в режиме multipart/mixed