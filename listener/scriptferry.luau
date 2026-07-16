local CONFIG = {
	baseUrl = "http://127.0.0.1:6767",
	token = "PASTE_TOKEN_HERE",
	silentMode = true,
	pollTimeoutMs = 25_000,
	retryDelaySeconds = 1,
}

local print, warn, error = print, warn, error
if CONFIG.silentMode then
	local noOp = function() end
	print, warn, error = noOp, noOp, noOp
end

if CONFIG.token == "" or CONFIG.token == "PASTE_TOKEN_HERE" then
	error("Paste the token from ScriptFerry: Copy Connection Snippet into CONFIG.token")
end
if type(loadstring) ~= "function" then
	error("This Luau environment must provide dynamic compilation through loadstring")
end

local cloneref = cloneref or function(ref)
	return ref
end

local HttpService = cloneref(game:GetService("HttpService"))
local encodedToken = HttpService:UrlEncode(CONFIG.token)
local running = true
local lastEventId = 0
local lastTerminalResult = nil
local pendingResult = nil
local nonce = 0

local function nextNonce()
	nonce += 1
	return nonce
end

local function httpGet(path)
	local ok, bodyOrError = pcall(function()
		return game:HttpGet(CONFIG.baseUrl .. path)
	end)
	if not ok then
		return nil, tostring(bodyOrError)
	end
	return bodyOrError, nil
end

local function decodeJson(body)
	local ok, valueOrError = pcall(function()
		return HttpService:JSONDecode(body)
	end)
	if not ok then
		return nil, tostring(valueOrError)
	end
	return valueOrError, nil
end

local function truncateForUrl(value)
	local text = tostring(value)
	if #text <= 4_096 then
		return text
	end

	local truncated = string.sub(text, 1, 4_096)
	while #truncated > 0 and utf8.len(truncated) == nil do
		truncated = string.sub(truncated, 1, #truncated - 1)
	end
	return truncated .. "... [truncated]"
end

local function execute(source, name)
	local compiled, chunkOrError, compileError = pcall(loadstring, source, "=" .. name)
	if not compiled then
		return {
			success = false,
			error = "compile error: " .. tostring(chunkOrError),
		}
	end
	if type(chunkOrError) ~= "function" then
		return {
			success = false,
			error = "compile error: " .. tostring(compileError),
		}
	end

	local executed, valueOrError = pcall(chunkOrError)
	if not executed then
		return {
			success = false,
			error = "runtime error: " .. tostring(valueOrError),
		}
	end
	return {
		success = true,
		output = valueOrError == nil and "" or tostring(valueOrError),
	}
end

local function validateEventEnvelope(envelope)
	if type(envelope) ~= "table" or envelope.protocol ~= 1 then
		return nil, "response has an unsupported protocol version"
	end
	if envelope.timeout == true then
		return nil, nil
	end

	local event = envelope.event
	if type(event) ~= "table" then
		return nil, "response is missing an event object"
	end
	if
		type(event.id) ~= "number"
		or event.id < 1
		or event.id % 1 ~= 0
		or event.type ~= "execute"
		or type(event.name) ~= "string"
		or type(event.languageId) ~= "string"
		or type(event.source) ~= "string"
	then
		return nil, "response contains an invalid execute event"
	end
	return event, nil
end

local function stop()
	running = false
end

_G.ScriptFerryStop = stop

local function backoff(message)
	warn("ScriptFerry listener: " .. message)
	if running then
		task.wait(CONFIG.retryDelaySeconds)
	end
end

local function submitPendingResult()
	local result = pendingResult.result
	local query = "?token="
		.. encodedToken
		.. "&nonce="
		.. tostring(nextNonce())
		.. "&success="
		.. tostring(result.success)
	if result.success then
		query ..= "&output=" .. HttpService:UrlEncode(truncateForUrl(result.output or ""))
	else
		query ..= "&error=" .. HttpService:UrlEncode(truncateForUrl(result.error or "Unknown error"))
	end

	local body, transportError = httpGet("/v1/events/" .. tostring(pendingResult.id) .. "/result" .. query)
	if body == nil then
		backoff("could not report event " .. tostring(pendingResult.id) .. ": " .. transportError)
		return
	end

	local response, decodeError = decodeJson(body)
	if type(response) ~= "table" or (response.status ~= "accepted" and response.status ~= "duplicate") then
		backoff("result endpoint returned an invalid response: " .. tostring(decodeError or body))
		return
	end

	lastEventId = math.max(lastEventId, pendingResult.id)
	lastTerminalResult = pendingResult.result
	if not pendingResult.duplicate then
		print("ScriptFerry listener: reported terminal result for event " .. tostring(pendingResult.id))
	end
	pendingResult = nil
end

while running do
	if pendingResult ~= nil then
		submitPendingResult()
		continue
	end

	local pollPath = "/v1/events?after="
		.. tostring(lastEventId)
		.. "&timeout="
		.. tostring(CONFIG.pollTimeoutMs)
		.. "&transport=game-httpget&nonce="
		.. tostring(nextNonce())
		.. "&token="
		.. encodedToken
	local body, transportError = httpGet(pollPath)
	if body == nil then
		backoff("poll failed: " .. transportError)
	else
		local envelope, decodeError = decodeJson(body)
		if envelope == nil then
			backoff("poll returned malformed JSON: " .. decodeError)
		else
			local event, protocolError = validateEventEnvelope(envelope)
			if protocolError ~= nil then
				backoff(protocolError)
			elseif event ~= nil and event.id <= lastEventId then
				if event.id == lastEventId and lastTerminalResult ~= nil then
					pendingResult = {
						id = event.id,
						result = lastTerminalResult,
						duplicate = true,
					}
				end
			elseif event ~= nil then
				pendingResult = {
					id = event.id,
					result = execute(event.source, event.name),
					duplicate = false,
				}
			end
		end
	end
end

print("ScriptFerry listener: stopped")
