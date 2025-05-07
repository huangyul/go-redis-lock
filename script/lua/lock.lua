local key = KEYS[1]
local value = ARGV[1]
local expiration = ARGV[2]

local val = redis.call("get", key)

if val == false then
  return redis.call("set", key, value, "EX", expiration)
elseif val == value then
  redis.call("expire", key, expiration)
  return "OK"
else
  return ""
end
