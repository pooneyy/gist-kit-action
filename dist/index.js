import*as A from"os";import e from"os";import*as t from"crypto";import*as r from"fs";import{promises as n}from"fs";import*as s from"path";import o from"http";import i from"https";import"net";import a from"tls";import g from"events";import c from"assert";import Q from"util";import E from"node:assert";import B from"node:net";import h from"node:http";import I from"node:stream";import C from"node:buffer";import l from"node:util";import u from"node:querystring";import d from"node:events";import f from"node:diagnostics_channel";import p from"node:tls";import w from"node:zlib";import y from"node:perf_hooks";import D from"node:util/types";import m from"node:worker_threads";import k from"node:url";import R from"node:async_hooks";import b from"node:console";import F from"node:dns";import S from"string_decoder";import"child_process";import"timers";import"stream";import*as N from"node:path";import{readFile as M,mkdir as U,writeFile as L}from"node:fs/promises";
// We use any as a valid input type
/* eslint-disable @typescript-eslint/no-explicit-any */
/**
 * Sanitizes an input into a string so it can be passed into issueCommand safely
 * @param input input to sanitize into a string
 */
function T(A){return null==A?"":"string"==typeof A||A instanceof String?A:JSON.stringify(A)}
/**
 *
 * @param annotationProperties
 * @returns The command properties to send with the actual annotation command
 * See IssueCommandProperties: https://github.com/actions/runner/blob/main/src/Runner.Worker/ActionCommandManager.cs#L646
 */function G(A){return Object.keys(A).length?{title:A.title,file:A.file,line:A.startLine,endLine:A.endLine,col:A.startColumn,endColumn:A.endColumn}:{}}
/**
 * Issues a command to the GitHub Actions runner
 *
 * @param command - The command name to issue
 * @param properties - Additional properties for the command (key-value pairs)
 * @param message - The message to include with the command
 * @remarks
 * This function outputs a specially formatted string to stdout that the Actions
 * runner interprets as a command. These commands can control workflow behavior,
 * set outputs, create annotations, mask values, and more.
 *
 * Command Format:
 *   ::name key=value,key=value::message
 *
 * @example
 * ```typescript
 * // Issue a warning annotation
 * issueCommand('warning', {}, 'This is a warning message');
 * // Output: ::warning::This is a warning message
 *
 * // Set an environment variable
 * issueCommand('set-env', { name: 'MY_VAR' }, 'some value');
 * // Output: ::set-env name=MY_VAR::some value
 *
 * // Add a secret mask
 * issueCommand('add-mask', {}, 'secretValue123');
 * // Output: ::add-mask::secretValue123
 * ```
 *
 * @internal
 * This is an internal utility function that powers the public API functions
 * such as setSecret, warning, error, and exportVariable.
 */function Y(e,t,r){const n=new v(e,t,r);process.stdout.write(n.toString()+A.EOL)}class v{constructor(A,e,t){A||(A="missing.command"),this.command=A,this.properties=e,this.message=t}toString(){let A="::"+this.command;if(this.properties&&Object.keys(this.properties).length>0){A+=" ";let e=!0;for(const t in this.properties)if(this.properties.hasOwnProperty(t)){const r=this.properties[t];r&&(e?e=!1:A+=",",A+=`${t}=${J(r)}`)}}var e;return A+=`::${e=this.message,T(e).replace(/%/g,"%25").replace(/\r/g,"%0D").replace(/\n/g,"%0A")}`,A}}function J(A){return T(A).replace(/%/g,"%25").replace(/\r/g,"%0D").replace(/\n/g,"%0A").replace(/:/g,"%3A").replace(/,/g,"%2C")}
// For internal use, subject to change.
// We use any as a valid input type
/* eslint-disable @typescript-eslint/no-explicit-any */var H,V,x="undefined"!=typeof globalThis?globalThis:"undefined"!=typeof window?window:"undefined"!=typeof global?global:"undefined"!=typeof self?self:{},W={};V||(V=1,function(){if(H)return W;H=1;var A,e=a,t=o,r=i,n=g,s=Q;function c(A){var e=this;e.options=A||{},e.proxyOptions=e.options.proxy||{},e.maxSockets=e.options.maxSockets||t.Agent.defaultMaxSockets,e.requests=[],e.sockets=[],e.on("free",function(A,t,r,n){for(var s=B(t,r,n),o=0,i=e.requests.length;o<i;++o){var a=e.requests[o];if(a.host===s.host&&a.port===s.port)
// Detect the request to connect same origin server,
// reuse the connection.
return e.requests.splice(o,1),void a.request.onSocket(A)}A.destroy(),e.removeSocket(A)})}function E(A,t){var r=this;c.prototype.createSocket.call(r,A,function(n){var s=A.request.getHeader("host"),o=h({},r.options,{socket:n,servername:s?s.replace(/:.*$/,""):A.host}),i=e.connect(0,o);r.sockets[r.sockets.indexOf(n)]=i,t(i)})}function B(A,e,t){return"string"==typeof A?{host:A,port:e,localAddress:t}:A}function h(A){for(var e=1,t=arguments.length;e<t;++e){var r=arguments[e];if("object"==typeof r)for(var n=Object.keys(r),s=0,o=n.length;s<o;++s){var i=n[s];void 0!==r[i]&&(A[i]=r[i])}}return A}// for test
W.httpOverHttp=function(A){var e=new c(A);return e.request=t.request,e},W.httpsOverHttp=function(A){var e=new c(A);return e.request=t.request,e.createSocket=E,e.defaultPort=443,e},W.httpOverHttps=function(A){var e=new c(A);return e.request=r.request,e},W.httpsOverHttps=function(A){var e=new c(A);return e.request=r.request,e.createSocket=E,e.defaultPort=443,e},s.inherits(c,n.EventEmitter),c.prototype.addRequest=function(A,e,t,r){var n=this,s=h({request:A},n.options,B(e,t,r));n.sockets.length>=this.maxSockets?
// We are over limit so we'll add it to the queue.
n.requests.push(s):
// If we are under maxSockets create a new one.
n.createSocket(s,function(e){function t(){n.emit("free",e,s)}function r(A){n.removeSocket(e),e.removeListener("free",t),e.removeListener("close",r),e.removeListener("agentRemove",r)}e.on("free",t),e.on("close",r),e.on("agentRemove",r),A.onSocket(e)})},c.prototype.createSocket=function(e,t){var r=this,n={};r.sockets.push(n);var s=h({},r.proxyOptions,{method:"CONNECT",path:e.host+":"+e.port,agent:!1,headers:{host:e.host+":"+e.port}});e.localAddress&&(s.localAddress=e.localAddress),s.proxyAuth&&(s.headers=s.headers||{},s.headers["Proxy-Authorization"]="Basic "+new Buffer(s.proxyAuth).toString("base64")),A("making CONNECT request");var o=r.request(s);function i(s,i,a){var g;return o.removeAllListeners(),i.removeAllListeners(),200!==s.statusCode?(A("tunneling socket could not be established, statusCode=%d",s.statusCode),i.destroy(),(g=new Error("tunneling socket could not be established, statusCode="+s.statusCode)).code="ECONNRESET",e.request.emit("error",g),void r.removeSocket(n)):a.length>0?(A("got illegal response body from proxy"),i.destroy(),(g=new Error("got illegal response body from proxy")).code="ECONNRESET",e.request.emit("error",g),void r.removeSocket(n)):(A("tunneling connection has established"),r.sockets[r.sockets.indexOf(n)]=i,t(i))}o.useChunkedEncodingByDefault=!1,// for v0.6
o.once("response",function(A){
// Very hacky. This is necessary to avoid http-parser leaks.
A.upgrade=!0}),// for v0.6
o.once("upgrade",function(A,e,t){
// Hacky.
process.nextTick(function(){i(A,e,t)})}),// for v0.6
o.once("connect",i),// for v0.7 or later
o.once("error",function(t){o.removeAllListeners(),A("tunneling socket could not be established, cause=%s\n",t.message,t.stack);var s=new Error("tunneling socket could not be established, cause="+t.message);s.code="ECONNRESET",e.request.emit("error",s),r.removeSocket(n)}),o.end()},c.prototype.removeSocket=function(A){var e=this.sockets.indexOf(A);if(-1!==e){this.sockets.splice(e,1);var t=this.requests.shift();t&&
// If we have pending requests and a socket gets closed a new one
// needs to be created to take over in the pool for the one that closed.
this.createSocket(t,function(A){t.request.onSocket(A)})}},A=process.env.NODE_DEBUG&&/\btunnel\b/.test(process.env.NODE_DEBUG)?function(){var A=Array.prototype.slice.call(arguments);"string"==typeof A[0]?A[0]="TUNNEL: "+A[0]:A.unshift("TUNNEL:"),console.error.apply(console,A)}:function(){},W.debug=A}());var O,P,q,Z,z,K,X,_,j,$,AA,eA,tA,rA,nA,sA,oA,iA,aA,gA,cA,QA,EA={};function BA(){return P?O:(P=1,O={kClose:Symbol("close"),kDestroy:Symbol("destroy"),kDispatch:Symbol("dispatch"),kUrl:Symbol("url"),kWriting:Symbol("writing"),kResuming:Symbol("resuming"),kQueue:Symbol("queue"),kConnect:Symbol("connect"),kConnecting:Symbol("connecting"),kKeepAliveDefaultTimeout:Symbol("default keep alive timeout"),kKeepAliveMaxTimeout:Symbol("max keep alive timeout"),kKeepAliveTimeoutThreshold:Symbol("keep alive timeout threshold"),kKeepAliveTimeoutValue:Symbol("keep alive timeout"),kKeepAlive:Symbol("keep alive"),kHeadersTimeout:Symbol("headers timeout"),kBodyTimeout:Symbol("body timeout"),kServerName:Symbol("server name"),kLocalAddress:Symbol("local address"),kHost:Symbol("host"),kNoRef:Symbol("no ref"),kBodyUsed:Symbol("used"),kBody:Symbol("abstracted request body"),kRunning:Symbol("running"),kBlocking:Symbol("blocking"),kPending:Symbol("pending"),kSize:Symbol("size"),kBusy:Symbol("busy"),kQueued:Symbol("queued"),kFree:Symbol("free"),kConnected:Symbol("connected"),kClosed:Symbol("closed"),kNeedDrain:Symbol("need drain"),kReset:Symbol("reset"),kDestroyed:Symbol.for("nodejs.stream.destroyed"),kResume:Symbol("resume"),kOnError:Symbol("on error"),kMaxHeadersSize:Symbol("max headers size"),kRunningIdx:Symbol("running index"),kPendingIdx:Symbol("pending index"),kError:Symbol("error"),kClients:Symbol("clients"),kClient:Symbol("client"),kParser:Symbol("parser"),kOnDestroyed:Symbol("destroy callbacks"),kPipelining:Symbol("pipelining"),kSocket:Symbol("socket"),kHostHeader:Symbol("host header"),kConnector:Symbol("connector"),kStrictContentLength:Symbol("strict content length"),kMaxRedirections:Symbol("maxRedirections"),kMaxRequests:Symbol("maxRequestsPerClient"),kProxy:Symbol("proxy agent options"),kCounter:Symbol("socket request counter"),kInterceptors:Symbol("dispatch interceptors"),kMaxResponseSize:Symbol("max response size"),kHTTP2Session:Symbol("http2Session"),kHTTP2SessionState:Symbol("http2Session state"),kRetryHandlerDefaultRetry:Symbol("retry agent default retry"),kConstruct:Symbol("constructable"),kListeners:Symbol("listeners"),kHTTPContext:Symbol("http context"),kMaxConcurrentStreams:Symbol("max concurrent streams"),kNoProxyAgent:Symbol("no proxy agent"),kHttpProxyAgent:Symbol("http proxy agent"),kHttpsProxyAgent:Symbol("https proxy agent")})}function hA(){if(Z)return q;Z=1;const A=Symbol.for("undici.error.UND_ERR");class e extends Error{constructor(A){super(A),this.name="UndiciError",this.code="UND_ERR"}static[Symbol.hasInstance](e){return e&&!0===e[A]}[A]=!0}const t=Symbol.for("undici.error.UND_ERR_CONNECT_TIMEOUT");class r extends e{constructor(A){super(A),this.name="ConnectTimeoutError",this.message=A||"Connect Timeout Error",this.code="UND_ERR_CONNECT_TIMEOUT"}static[Symbol.hasInstance](A){return A&&!0===A[t]}[t]=!0}const n=Symbol.for("undici.error.UND_ERR_HEADERS_TIMEOUT");class s extends e{constructor(A){super(A),this.name="HeadersTimeoutError",this.message=A||"Headers Timeout Error",this.code="UND_ERR_HEADERS_TIMEOUT"}static[Symbol.hasInstance](A){return A&&!0===A[n]}[n]=!0}const o=Symbol.for("undici.error.UND_ERR_HEADERS_OVERFLOW");class i extends e{constructor(A){super(A),this.name="HeadersOverflowError",this.message=A||"Headers Overflow Error",this.code="UND_ERR_HEADERS_OVERFLOW"}static[Symbol.hasInstance](A){return A&&!0===A[o]}[o]=!0}const a=Symbol.for("undici.error.UND_ERR_BODY_TIMEOUT");class g extends e{constructor(A){super(A),this.name="BodyTimeoutError",this.message=A||"Body Timeout Error",this.code="UND_ERR_BODY_TIMEOUT"}static[Symbol.hasInstance](A){return A&&!0===A[a]}[a]=!0}const c=Symbol.for("undici.error.UND_ERR_RESPONSE_STATUS_CODE");class Q extends e{constructor(A,e,t,r){super(A),this.name="ResponseStatusCodeError",this.message=A||"Response Status Code Error",this.code="UND_ERR_RESPONSE_STATUS_CODE",this.body=r,this.status=e,this.statusCode=e,this.headers=t}static[Symbol.hasInstance](A){return A&&!0===A[c]}[c]=!0}const E=Symbol.for("undici.error.UND_ERR_INVALID_ARG");class B extends e{constructor(A){super(A),this.name="InvalidArgumentError",this.message=A||"Invalid Argument Error",this.code="UND_ERR_INVALID_ARG"}static[Symbol.hasInstance](A){return A&&!0===A[E]}[E]=!0}const h=Symbol.for("undici.error.UND_ERR_INVALID_RETURN_VALUE");class I extends e{constructor(A){super(A),this.name="InvalidReturnValueError",this.message=A||"Invalid Return Value Error",this.code="UND_ERR_INVALID_RETURN_VALUE"}static[Symbol.hasInstance](A){return A&&!0===A[h]}[h]=!0}const C=Symbol.for("undici.error.UND_ERR_ABORT");class l extends e{constructor(A){super(A),this.name="AbortError",this.message=A||"The operation was aborted",this.code="UND_ERR_ABORT"}static[Symbol.hasInstance](A){return A&&!0===A[C]}[C]=!0}const u=Symbol.for("undici.error.UND_ERR_ABORTED");class d extends l{constructor(A){super(A),this.name="AbortError",this.message=A||"Request aborted",this.code="UND_ERR_ABORTED"}static[Symbol.hasInstance](A){return A&&!0===A[u]}[u]=!0}const f=Symbol.for("undici.error.UND_ERR_INFO");class p extends e{constructor(A){super(A),this.name="InformationalError",this.message=A||"Request information",this.code="UND_ERR_INFO"}static[Symbol.hasInstance](A){return A&&!0===A[f]}[f]=!0}const w=Symbol.for("undici.error.UND_ERR_REQ_CONTENT_LENGTH_MISMATCH");class y extends e{constructor(A){super(A),this.name="RequestContentLengthMismatchError",this.message=A||"Request body length does not match content-length header",this.code="UND_ERR_REQ_CONTENT_LENGTH_MISMATCH"}static[Symbol.hasInstance](A){return A&&!0===A[w]}[w]=!0}const D=Symbol.for("undici.error.UND_ERR_RES_CONTENT_LENGTH_MISMATCH");class m extends e{constructor(A){super(A),this.name="ResponseContentLengthMismatchError",this.message=A||"Response body length does not match content-length header",this.code="UND_ERR_RES_CONTENT_LENGTH_MISMATCH"}static[Symbol.hasInstance](A){return A&&!0===A[D]}[D]=!0}const k=Symbol.for("undici.error.UND_ERR_DESTROYED");class R extends e{constructor(A){super(A),this.name="ClientDestroyedError",this.message=A||"The client is destroyed",this.code="UND_ERR_DESTROYED"}static[Symbol.hasInstance](A){return A&&!0===A[k]}[k]=!0}const b=Symbol.for("undici.error.UND_ERR_CLOSED");class F extends e{constructor(A){super(A),this.name="ClientClosedError",this.message=A||"The client is closed",this.code="UND_ERR_CLOSED"}static[Symbol.hasInstance](A){return A&&!0===A[b]}[b]=!0}const S=Symbol.for("undici.error.UND_ERR_SOCKET");class N extends e{constructor(A,e){super(A),this.name="SocketError",this.message=A||"Socket error",this.code="UND_ERR_SOCKET",this.socket=e}static[Symbol.hasInstance](A){return A&&!0===A[S]}[S]=!0}const M=Symbol.for("undici.error.UND_ERR_NOT_SUPPORTED");class U extends e{constructor(A){super(A),this.name="NotSupportedError",this.message=A||"Not supported error",this.code="UND_ERR_NOT_SUPPORTED"}static[Symbol.hasInstance](A){return A&&!0===A[M]}[M]=!0}const L=Symbol.for("undici.error.UND_ERR_BPL_MISSING_UPSTREAM");class T extends e{constructor(A){super(A),this.name="MissingUpstreamError",this.message=A||"No upstream has been added to the BalancedPool",this.code="UND_ERR_BPL_MISSING_UPSTREAM"}static[Symbol.hasInstance](A){return A&&!0===A[L]}[L]=!0}const G=Symbol.for("undici.error.UND_ERR_HTTP_PARSER");class Y extends Error{constructor(A,e,t){super(A),this.name="HTTPParserError",this.code=e?`HPE_${e}`:void 0,this.data=t?t.toString():void 0}static[Symbol.hasInstance](A){return A&&!0===A[G]}[G]=!0}const v=Symbol.for("undici.error.UND_ERR_RES_EXCEEDED_MAX_SIZE");class J extends e{constructor(A){super(A),this.name="ResponseExceededMaxSizeError",this.message=A||"Response content exceeded max size",this.code="UND_ERR_RES_EXCEEDED_MAX_SIZE"}static[Symbol.hasInstance](A){return A&&!0===A[v]}[v]=!0}const H=Symbol.for("undici.error.UND_ERR_REQ_RETRY");class V extends e{constructor(A,e,{headers:t,data:r}){super(A),this.name="RequestRetryError",this.message=A||"Request retry error",this.code="UND_ERR_REQ_RETRY",this.statusCode=e,this.data=r,this.headers=t}static[Symbol.hasInstance](A){return A&&!0===A[H]}[H]=!0}const x=Symbol.for("undici.error.UND_ERR_RESPONSE");class W extends e{constructor(A,e,{headers:t,data:r}){super(A),this.name="ResponseError",this.message=A||"Response error",this.code="UND_ERR_RESPONSE",this.statusCode=e,this.data=r,this.headers=t}static[Symbol.hasInstance](A){return A&&!0===A[x]}[x]=!0}const O=Symbol.for("undici.error.UND_ERR_PRX_TLS");class P extends e{constructor(A,e,t){super(e,{cause:A,...t??{}}),this.name="SecureProxyConnectionError",this.message=e||"Secure Proxy Connection failed",this.code="UND_ERR_PRX_TLS",this.cause=A}static[Symbol.hasInstance](A){return A&&!0===A[O]}[O]=!0}const z=Symbol.for("undici.error.UND_ERR_WS_MESSAGE_SIZE_EXCEEDED");class K extends e{constructor(A){super(A),this.name="MessageSizeExceededError",this.message=A||"Max decompressed message size exceeded",this.code="UND_ERR_WS_MESSAGE_SIZE_EXCEEDED"}static[Symbol.hasInstance](A){return A&&!0===A[z]}get[z](){return!0}}return q={AbortError:l,HTTPParserError:Y,UndiciError:e,HeadersTimeoutError:s,HeadersOverflowError:i,BodyTimeoutError:g,RequestContentLengthMismatchError:y,ConnectTimeoutError:r,ResponseStatusCodeError:Q,InvalidArgumentError:B,InvalidReturnValueError:I,RequestAbortedError:d,ClientDestroyedError:R,ClientClosedError:F,InformationalError:p,SocketError:N,NotSupportedError:U,ResponseContentLengthMismatchError:m,BalancedPoolMissingUpstreamError:T,ResponseExceededMaxSizeError:J,RequestRetryError:V,ResponseError:W,SecureProxyConnectionError:P,MessageSizeExceededError:K}}function IA(){if(K)return z;K=1;
/** @type {Record<string, string | undefined>} */
const A={},e=["Accept","Accept-Encoding","Accept-Language","Accept-Ranges","Access-Control-Allow-Credentials","Access-Control-Allow-Headers","Access-Control-Allow-Methods","Access-Control-Allow-Origin","Access-Control-Expose-Headers","Access-Control-Max-Age","Access-Control-Request-Headers","Access-Control-Request-Method","Age","Allow","Alt-Svc","Alt-Used","Authorization","Cache-Control","Clear-Site-Data","Connection","Content-Disposition","Content-Encoding","Content-Language","Content-Length","Content-Location","Content-Range","Content-Security-Policy","Content-Security-Policy-Report-Only","Content-Type","Cookie","Cross-Origin-Embedder-Policy","Cross-Origin-Opener-Policy","Cross-Origin-Resource-Policy","Date","Device-Memory","Downlink","ECT","ETag","Expect","Expect-CT","Expires","Forwarded","From","Host","If-Match","If-Modified-Since","If-None-Match","If-Range","If-Unmodified-Since","Keep-Alive","Last-Modified","Link","Location","Max-Forwards","Origin","Permissions-Policy","Pragma","Proxy-Authenticate","Proxy-Authorization","RTT","Range","Referer","Referrer-Policy","Refresh","Retry-After","Sec-WebSocket-Accept","Sec-WebSocket-Extensions","Sec-WebSocket-Key","Sec-WebSocket-Protocol","Sec-WebSocket-Version","Server","Server-Timing","Service-Worker-Allowed","Service-Worker-Navigation-Preload","Set-Cookie","SourceMap","Strict-Transport-Security","Supports-Loading-Mode","TE","Timing-Allow-Origin","Trailer","Transfer-Encoding","Upgrade","Upgrade-Insecure-Requests","User-Agent","Vary","Via","WWW-Authenticate","X-Content-Type-Options","X-DNS-Prefetch-Control","X-Frame-Options","X-Permitted-Cross-Domain-Policies","X-Powered-By","X-Requested-With","X-XSS-Protection"];
// https://developer.mozilla.org/docs/Web/HTTP/Headers
for(let t=0;t<e.length;++t){const r=e[t],n=r.toLowerCase();A[r]=A[n]=n}
// Note: object prototypes should not be able to be referenced. e.g. `Object#hasOwnProperty`.
return Object.setPrototypeOf(A,null),z={wellknownHeaderNames:e,headerNameLowerCasedRecord:A}}function CA(){if($)return j;$=1;const A=E,{kDestroyed:e,kBodyUsed:t,kListeners:r,kBody:n}=BA(),{IncomingMessage:s}=h,o=I,i=B,{Blob:a}=C,g=l,{stringify:c}=u,{EventEmitter:Q}=d,{InvalidArgumentError:f}=hA(),{headerNameLowerCasedRecord:p}=IA(),{tree:w}=function(){if(_)return X;_=1;const{wellknownHeaderNames:A,headerNameLowerCasedRecord:e}=IA();class t{
/** @type {any} */
value=null
/** @type {null | TstNode} */;left=null
/** @type {null | TstNode} */;middle=null
/** @type {null | TstNode} */;right=null
/** @type {number} */;code;
/**
	   * @param {string} key
	   * @param {any} value
	   * @param {number} index
	   */constructor(A,e,r){if(void 0===r||r>=A.length)throw new TypeError("Unreachable");
// check code is ascii string
if((this.code=A.charCodeAt(r))>127)throw new TypeError("key must be ascii string");A.length!==++r?this.middle=new t(A,e,r):this.value=e}
/**
	   * @param {string} key
	   * @param {any} value
	   */add(A,e){const r=A.length;if(0===r)throw new TypeError("Unreachable");let n=0,s=this;for(;;){const o=A.charCodeAt(n);
// check code is ascii string
if(o>127)throw new TypeError("key must be ascii string");if(s.code===o){if(r===++n){s.value=e;break}if(null===s.middle){s.middle=new t(A,e,n);break}s=s.middle}else if(s.code<o){if(null===s.left){s.left=new t(A,e,n);break}s=s.left}else{if(null===s.right){s.right=new t(A,e,n);break}s=s.right}}}
/**
	   * @param {Uint8Array} key
	   * @return {TstNode | null}
	   */search(A){const e=A.length;let t=0,r=this;for(;null!==r&&t<e;){let n=A[t];
// A-Z
// First check if it is bigger than 0x5a.
// Lowercase letters have higher char codes than uppercase ones.
// Also we assume that headers will mostly contain lowercase characters.
for(n<=90&&n>=65&&(
// Lowercase for uppercase.
n|=32);null!==r;){if(n===r.code){if(e===++t)
// Returns Node since it is the last key.
return r;r=r.middle;break}r=r.code<n?r.left:r.right}}return null}}class r{
/** @type {TstNode | null} */
node=null
/**
	   * @param {string} key
	   * @param {any} value
	   * */;insert(A,e){null===this.node?this.node=new t(A,e,0):this.node.add(A,e)}
/**
	   * @param {Uint8Array} key
	   * @return {any}
	   */lookup(A){return this.node?.search(A)?.value??null}}const n=new r;for(let t=0;t<A.length;++t){const r=e[A[t]];n.insert(r,r)}return X={TernarySearchTree:r,tree:n}}(),[y,D]=process.versions.node.split(".").map(A=>Number(A));class m{constructor(A){this[n]=A,this[t]=!1}async*[Symbol.asyncIterator](){A(!this[t],"disturbed"),this[t]=!0,yield*this[n]}}function k(A){return A&&"object"==typeof A&&"function"==typeof A.pipe&&"function"==typeof A.on}
// based on https://github.com/node-fetch/fetch-blob/blob/8ab587d34080de94140b54f07168451e7d0b655e/index.js#L229-L241 (MIT License)
function R(A){if(null===A)return!1;if(A instanceof a)return!0;if("object"!=typeof A)return!1;{const e=A[Symbol.toStringTag];return("Blob"===e||"File"===e)&&("stream"in A&&"function"==typeof A.stream||"arrayBuffer"in A&&"function"==typeof A.arrayBuffer)}}function b(A){const e=parseInt(A,10);return e===Number(A)&&e>=0&&e<=65535}function F(A){return null!=A&&"h"===A[0]&&"t"===A[1]&&"t"===A[2]&&"p"===A[3]&&(":"===A[4]||"s"===A[4]&&":"===A[5])}function S(A){if("string"==typeof A){if(!F((A=new URL(A)).origin||A.protocol))throw new f("Invalid URL protocol: the URL must start with `http:` or `https:`.");return A}if(!A||"object"!=typeof A)throw new f("Invalid URL: The URL argument must be a non-null object.");if(!(A instanceof URL)){if(null!=A.port&&""!==A.port&&!1===b(A.port))throw new f("Invalid URL: port must be a valid integer or a string representation of an integer.");if(null!=A.path&&"string"!=typeof A.path)throw new f("Invalid URL path: the path must be a string or null/undefined.");if(null!=A.pathname&&"string"!=typeof A.pathname)throw new f("Invalid URL pathname: the pathname must be a string or null/undefined.");if(null!=A.hostname&&"string"!=typeof A.hostname)throw new f("Invalid URL hostname: the hostname must be a string or null/undefined.");if(null!=A.origin&&"string"!=typeof A.origin)throw new f("Invalid URL origin: the origin must be a string or null/undefined.");if(!F(A.origin||A.protocol))throw new f("Invalid URL protocol: the URL must start with `http:` or `https:`.");const e=null!=A.port?A.port:"https:"===A.protocol?443:80;let t=null!=A.origin?A.origin:`${A.protocol||""}//${A.hostname||""}:${e}`,r=null!=A.path?A.path:`${A.pathname||""}${A.search||""}`;
// new URL(path, origin) is unsafe when `path` contains an absolute URL
// From https://developer.mozilla.org/en-US/docs/Web/API/URL/URL:
// If first parameter is a relative URL, second param is required, and will be used as the base URL.
// If first parameter is an absolute URL, a given second param will be ignored.
return"/"===t[t.length-1]&&(t=t.slice(0,t.length-1)),r&&"/"!==r[0]&&(r=`/${r}`),new URL(`${t}${r}`)}if(!F(A.origin||A.protocol))throw new f("Invalid URL protocol: the URL must start with `http:` or `https:`.");return A}function N(A){return!(null==A||"function"!=typeof A[Symbol.iterator]&&"function"!=typeof A[Symbol.asyncIterator])}function M(A){if(null==A)return 0;if(k(A)){const e=A._readableState;return e&&!1===e.objectMode&&!0===e.ended&&Number.isFinite(e.length)?e.length:null}return R(A)?null!=A.size?A.size:null:G(A)?A.byteLength:null}function U(A){return A&&!!(A.destroyed||A[e]||o.isDestroyed?.(A))}const L=/timeout=(\d+)/;
/**
	 * Retrieves a header name and returns its lowercase value.
	 * @param {string | Buffer} value Header name
	 * @returns {string}
	 */
function T(A){return"string"==typeof A?p[A]??A.toLowerCase():w.lookup(A)??A.toString("latin1").toLowerCase()}
/**
	 * Receive the buffer as a string and return its lowercase value.
	 * @param {Buffer} value Header name
	 * @returns {string}
	 */function G(A){
// See, https://github.com/mcollina/undici/pull/319
return A instanceof Uint8Array||Buffer.isBuffer(A)}const Y="function"==typeof String.prototype.toWellFormed,v="function"==typeof String.prototype.isWellFormed;
/**
	 * @param {string} val
	 */
function J(A){return Y?`${A}`.toWellFormed():g.toUSVString(A)}
/**
	 * @param {string} val
	 */
// TODO: move this to webidl
/**
	 * @see https://tools.ietf.org/html/rfc7230#section-3.2.6
	 * @param {number} c
	 */
function H(A){switch(A){case 34:case 40:case 41:case 44:case 47:case 58:case 59:case 60:case 61:case 62:case 63:case 64:case 91:case 92:case 93:case 123:case 125:
// DQUOTE and "(),/:;<=>?@[\]{}"
return!1;default:
// VCHAR %x21-7E
return A>=33&&A<=126}}
/**
	 * @param {string} characters
	 */
// headerCharRegex have been lifted from
// https://github.com/nodejs/node/blob/main/lib/_http_common.js
/**
	 * Matches if val contains an invalid field-vchar
	 *  field-value    = *( field-content / obs-fold )
	 *  field-content  = field-vchar [ 1*( SP / HTAB ) field-vchar ]
	 *  field-vchar    = VCHAR / obs-text
	 */
const V=/[^\t\x20-\x7e\x80-\xff]/;
/**
	 * @param {string} characters
	 */const x=Object.create(null);x.enumerable=!0;const W={delete:"DELETE",DELETE:"DELETE",get:"GET",GET:"GET",head:"HEAD",HEAD:"HEAD",options:"OPTIONS",OPTIONS:"OPTIONS",post:"POST",POST:"POST",put:"PUT",PUT:"PUT"},O={...W,patch:"patch",PATCH:"PATCH"};
// Note: object prototypes should not be able to be referenced. e.g. `Object#hasOwnProperty`.
return Object.setPrototypeOf(W,null),Object.setPrototypeOf(O,null),j={kEnumerableProperty:x,nop:function(){},isDisturbed:
// A body is disturbed if it has been read from and it cannot
// be re-used without losing state or data.
function(A){
// TODO (fix): Why is body[kBodyUsed] needed?
return!(!A||!o.isDisturbed(A)&&!A[t])},isErrored:function(A){return!(!A||!o.isErrored(A))},isReadable:function(A){return!(!A||!o.isReadable(A))},toUSVString:J,isUSVString:function(A){return v?`${A}`.isWellFormed():J(A)===`${A}`},isBlobLike:R,parseOrigin:function(A){if("/"!==(A=S(A)).pathname||A.search||A.hash)throw new f("invalid url");return A},parseURL:S,getServerName:
// IP addresses are not valid server names per RFC6066
// > Currently, the only server names supported are DNS hostnames
function(e){if(!e)return null;A("string"==typeof e);const t=function(e){if("["===e[0]){const t=e.indexOf("]");return A(-1!==t),e.substring(1,t)}const t=e.indexOf(":");return-1===t?e:e.substring(0,t)}(e);return i.isIP(t)?"":t},isStream:k,isIterable:N,isAsyncIterable:function(A){return!(null==A||"function"!=typeof A[Symbol.asyncIterator])},isDestroyed:U,headerNameToString:T,bufferToLowerCasedHeaderName:function(A){return w.lookup(A)??A.toString("latin1").toLowerCase()}
/**
	 * @param {Record<string, string | string[]> | (Buffer | string | (Buffer | string)[])[]} headers
	 * @param {Record<string, string | string[]>} [obj]
	 * @returns {Record<string, string | string[]>}
	 */,addListener:function(A,e,t){return(A[r]??=[]).push([e,t]),A.on(e,t),A},removeAllListeners:function(A){for(const[e,t]of A[r]??[])A.removeListener(e,t);A[r]=null},errorRequest:function(e,t,r){try{t.onError(r),A(t.aborted)}catch(r){e.emit("error",r)}},parseRawHeaders:function(A){const e=A.length,t=new Array(e);let r,n,s=!1,o=-1,i=0;for(let e=0;e<A.length;e+=2)r=A[e],n=A[e+1],"string"!=typeof r&&(r=r.toString()),"string"!=typeof n&&(n=n.toString("utf8")),i=r.length,14!==i||"-"!==r[7]||"content-length"!==r&&"content-length"!==r.toLowerCase()?19!==i||"-"!==r[7]||"content-disposition"!==r&&"content-disposition"!==r.toLowerCase()||(o=e+1):s=!0,t[e]=r,t[e+1]=n;
// See https://github.com/nodejs/node/pull/46528
return s&&-1!==o&&(t[o]=Buffer.from(t[o]).toString("latin1")),t},parseHeaders:function(A,e){void 0===e&&(e={});for(let t=0;t<A.length;t+=2){const r=T(A[t]);let n=e[r];if(n)"string"==typeof n&&(n=[n],e[r]=n),n.push(A[t+1].toString("utf8"));else{const n=A[t+1];e[r]="string"==typeof n?n:Array.isArray(n)?n.map(A=>A.toString("utf8")):n.toString("utf8")}}
// See https://github.com/nodejs/node/pull/46528
return"content-length"in e&&"content-disposition"in e&&(e["content-disposition"]=Buffer.from(e["content-disposition"]).toString("latin1")),e},parseKeepAliveTimeout:function(A){const e=A.toString().match(L);return e?1e3*parseInt(e[1],10):null},destroy:function(A,t){null!=A&&k(A)&&!U(A)&&("function"==typeof A.destroy?(Object.getPrototypeOf(A).constructor===s&&(
// See: https://github.com/nodejs/node/pull/38505/files
A.socket=null),A.destroy(t)):t&&queueMicrotask(()=>{A.emit("error",t)}),!0!==A.destroyed&&(A[e]=!0))},bodyLength:M,deepClone:function(A){return JSON.parse(JSON.stringify(A))},ReadableStreamFrom:
/** @type {globalThis['ReadableStream']} */
function(A){
// We cannot use ReadableStream.from here because it does not return a byte stream.
let e;return new ReadableStream({async start(){e=A[Symbol.asyncIterator]()},async pull(A){const{done:t,value:r}=await e.next();if(t)queueMicrotask(()=>{A.close(),A.byobRequest?.respond(0)});else{const e=Buffer.isBuffer(r)?r:Buffer.from(r);e.byteLength&&A.enqueue(new Uint8Array(e))}return A.desiredSize>0},async cancel(A){await e.return()},type:"bytes"})}
// The chunk should be a FormData instance and contains
// all the required methods.
,isBuffer:G,validateHandler:function(A,e,t){if(!A||"object"!=typeof A)throw new f("handler must be an object");if("function"!=typeof A.onConnect)throw new f("invalid onConnect method");if("function"!=typeof A.onError)throw new f("invalid onError method");if("function"!=typeof A.onBodySent&&void 0!==A.onBodySent)throw new f("invalid onBodySent method");if(t||"CONNECT"===e){if("function"!=typeof A.onUpgrade)throw new f("invalid onUpgrade method")}else{if("function"!=typeof A.onHeaders)throw new f("invalid onHeaders method");if("function"!=typeof A.onData)throw new f("invalid onData method");if("function"!=typeof A.onComplete)throw new f("invalid onComplete method")}},getSocketInfo:function(A){return{localAddress:A.localAddress,localPort:A.localPort,remoteAddress:A.remoteAddress,remotePort:A.remotePort,remoteFamily:A.remoteFamily,timeout:A.timeout,bytesWritten:A.bytesWritten,bytesRead:A.bytesRead}},isFormDataLike:function(A){return A&&"object"==typeof A&&"function"==typeof A.append&&"function"==typeof A.delete&&"function"==typeof A.get&&"function"==typeof A.getAll&&"function"==typeof A.has&&"function"==typeof A.set&&"FormData"===A[Symbol.toStringTag]},buildURL:function(A,e){if(A.includes("?")||A.includes("#"))throw new Error('Query params cannot be passed when url already contains "?" or "#".');const t=c(e);return t&&(A+="?"+t),A},addAbortListener:function(A,e){return"addEventListener"in A?(A.addEventListener("abort",e,{once:!0}),()=>A.removeEventListener("abort",e)):(A.addListener("abort",e),()=>A.removeListener("abort",e))},isValidHTTPToken:function(A){if(0===A.length)return!1;for(let e=0;e<A.length;++e)if(!H(A.charCodeAt(e)))return!1;return!0},isValidHeaderValue:function(A){return!V.test(A)}
// Parsed accordingly to RFC 9110
// https://www.rfc-editor.org/rfc/rfc9110#field.content-range
,isTokenCharCode:H,parseRangeHeader:function(A){if(null==A||""===A)return{start:0,end:null,size:null};const e=A?A.match(/^bytes (\d+)-(\d+)\/(\d+)?$/):null;return e?{start:parseInt(e[1]),end:e[2]?parseInt(e[2]):null,size:e[3]?parseInt(e[3]):null}:null},normalizedMethodRecordsBase:W,normalizedMethodRecords:O,isValidPort:b,isHttpOrHttpsPrefixed:F,nodeMajor:y,nodeMinor:D,safeHTTPMethods:["GET","HEAD","OPTIONS","TRACE"],wrapRequestBody:function(e){return k(e)?(
// TODO (fix): Provide some way for the user to cache the file to e.g. /tmp
// so that it can be dispatched again?
// TODO (fix): Do we need 100-expect support to provide a way to do this properly?
0===M(e)&&e.on("data",function(){A(!1)}),"boolean"!=typeof e.readableDidRead&&(e[t]=!1,Q.prototype.on.call(e,"data",function(){this[t]=!0})),e):e&&"function"==typeof e.pipeTo||e&&"string"!=typeof e&&!ArrayBuffer.isView(e)&&N(e)?new m(e):e}},j}function lA(){if(eA)return AA;eA=1;const A=f,e=l,t=e.debuglog("undici"),r=e.debuglog("fetch"),n=e.debuglog("websocket");let s=!1;const o={
// Client
beforeConnect:A.channel("undici:client:beforeConnect"),connected:A.channel("undici:client:connected"),connectError:A.channel("undici:client:connectError"),sendHeaders:A.channel("undici:client:sendHeaders"),
// Request
create:A.channel("undici:request:create"),bodySent:A.channel("undici:request:bodySent"),headers:A.channel("undici:request:headers"),trailers:A.channel("undici:request:trailers"),error:A.channel("undici:request:error"),
// WebSocket
open:A.channel("undici:websocket:open"),close:A.channel("undici:websocket:close"),socketError:A.channel("undici:websocket:socket_error"),ping:A.channel("undici:websocket:ping"),pong:A.channel("undici:websocket:pong")};if(t.enabled||r.enabled){const e=r.enabled?r:t;
// Track all Client events
A.channel("undici:client:beforeConnect").subscribe(A=>{const{connectParams:{version:t,protocol:r,port:n,host:s}}=A;e("connecting to %s using %s%s",`${s}${n?`:${n}`:""}`,r,t)}),A.channel("undici:client:connected").subscribe(A=>{const{connectParams:{version:t,protocol:r,port:n,host:s}}=A;e("connected to %s using %s%s",`${s}${n?`:${n}`:""}`,r,t)}),A.channel("undici:client:connectError").subscribe(A=>{const{connectParams:{version:t,protocol:r,port:n,host:s},error:o}=A;e("connection to %s using %s%s errored - %s",`${s}${n?`:${n}`:""}`,r,t,o.message)}),A.channel("undici:client:sendHeaders").subscribe(A=>{const{request:{method:t,path:r,origin:n}}=A;e("sending request to %s %s/%s",t,n,r)}),
// Track Request events
A.channel("undici:request:headers").subscribe(A=>{const{request:{method:t,path:r,origin:n},response:{statusCode:s}}=A;e("received response to %s %s/%s - HTTP %d",t,n,r,s)}),A.channel("undici:request:trailers").subscribe(A=>{const{request:{method:t,path:r,origin:n}}=A;e("trailers received from %s %s/%s",t,n,r)}),A.channel("undici:request:error").subscribe(A=>{const{request:{method:t,path:r,origin:n},error:s}=A;e("request to %s %s/%s errored - %s",t,n,r,s.message)}),s=!0}if(n.enabled){if(!s){const e=t.enabled?t:n;A.channel("undici:client:beforeConnect").subscribe(A=>{const{connectParams:{version:t,protocol:r,port:n,host:s}}=A;e("connecting to %s%s using %s%s",s,n?`:${n}`:"",r,t)}),A.channel("undici:client:connected").subscribe(A=>{const{connectParams:{version:t,protocol:r,port:n,host:s}}=A;e("connected to %s%s using %s%s",s,n?`:${n}`:"",r,t)}),A.channel("undici:client:connectError").subscribe(A=>{const{connectParams:{version:t,protocol:r,port:n,host:s},error:o}=A;e("connection to %s%s using %s%s errored - %s",s,n?`:${n}`:"",r,t,o.message)}),A.channel("undici:client:sendHeaders").subscribe(A=>{const{request:{method:t,path:r,origin:n}}=A;e("sending request to %s %s/%s",t,n,r)})}
// Track all WebSocket events
A.channel("undici:websocket:open").subscribe(A=>{const{address:{address:e,port:t}}=A;n("connection opened %s%s",e,t?`:${t}`:"")}),A.channel("undici:websocket:close").subscribe(A=>{const{websocket:e,code:t,reason:r}=A;n("closed connection to %s - %s %s",e.url,t,r)}),A.channel("undici:websocket:socket_error").subscribe(A=>{n("connection errored - %s",A.message)}),A.channel("undici:websocket:ping").subscribe(A=>{n("ping received")}),A.channel("undici:websocket:pong").subscribe(A=>{n("pong received")})}return AA={channels:o}}function uA(){if(sA)return nA;sA=1;const A=d;class e extends A{dispatch(){throw new Error("not implemented")}close(){throw new Error("not implemented")}destroy(){throw new Error("not implemented")}compose(...A){
// So we handle [interceptor1, interceptor2] or interceptor1, interceptor2, ...
const e=Array.isArray(A[0])?A[0]:A;let r=this.dispatch.bind(this);for(const A of e)if(null!=A){if("function"!=typeof A)throw new TypeError("invalid interceptor, expected function received "+typeof A);if(r=A(r),null==r||"function"!=typeof r||2!==r.length)throw new TypeError("invalid interceptor")}return new t(this,r)}}class t extends e{#A=null;#e=null;constructor(A,e){super(),this.#A=A,this.#e=e}dispatch(...A){this.#e(...A)}close(...A){return this.#A.close(...A)}destroy(...A){return this.#A.destroy(...A)}}return nA=e}function dA(){if(iA)return oA;iA=1;const A=uA(),{ClientDestroyedError:e,ClientClosedError:t,InvalidArgumentError:r}=hA(),{kDestroy:n,kClose:s,kClosed:o,kDestroyed:i,kDispatch:a,kInterceptors:g}=BA(),c=Symbol("onDestroyed"),Q=Symbol("onClosed"),E=Symbol("Intercepted Dispatch"),B=Symbol("webSocketOptions");return oA=class extends A{constructor(A){super(),this[i]=!1,this[c]=null,this[o]=!1,this[Q]=[],this[B]=A?.webSocket??{}}get webSocketOptions(){return{maxFragments:this[B].maxFragments??131072,maxPayloadSize:this[B].maxPayloadSize??134217728}}get destroyed(){return this[i]}get closed(){return this[o]}get interceptors(){return this[g]}set interceptors(A){if(A)for(let e=A.length-1;e>=0;e--){if("function"!=typeof this[g][e])throw new r("interceptor must be an function")}this[g]=A}close(A){if(void 0===A)return new Promise((A,e)=>{this.close((t,r)=>t?e(t):A(r))});if("function"!=typeof A)throw new r("invalid callback");if(this[i])return void queueMicrotask(()=>A(new e,null));if(this[o])return void(this[Q]?this[Q].push(A):queueMicrotask(()=>A(null,null)));this[o]=!0,this[Q].push(A);const t=()=>{const A=this[Q];this[Q]=null;for(let e=0;e<A.length;e++)A[e](null,null)};
// Should not error.
this[s]().then(()=>this.destroy()).then(()=>{queueMicrotask(t)})}destroy(A,t){if("function"==typeof A&&(t=A,A=null),void 0===t)return new Promise((e,t)=>{this.destroy(A,(A,r)=>A?/* istanbul ignore next: should never error */t(A):e(r))});if("function"!=typeof t)throw new r("invalid callback");if(this[i])return void(this[c]?this[c].push(t):queueMicrotask(()=>t(null,null)));A||(A=new e),this[i]=!0,this[c]=this[c]||[],this[c].push(t);const s=()=>{const A=this[c];this[c]=null;for(let e=0;e<A.length;e++)A[e](null,null)};
// Should not error.
this[n](A).then(()=>{queueMicrotask(s)})}[E](A,e){if(!this[g]||0===this[g].length)return this[E]=this[a],this[a](A,e);let t=this[a].bind(this);for(let A=this[g].length-1;A>=0;A--)t=this[g][A](t);return this[E]=t,t(A,e)}dispatch(A,n){if(!n||"object"!=typeof n)throw new r("handler must be an object");try{if(!A||"object"!=typeof A)throw new r("opts must be an object.");if(this[i]||this[c])throw new e;if(this[o])throw new t;return this[E](A,n)}catch(A){if("function"!=typeof n.onError)throw new r("invalid onError method");return n.onError(A),!1}}}}function fA(){if(gA)return aA;gA=1;
/**
	 * This module offers an optimized timer implementation designed for scenarios
	 * where high precision is not critical.
	 *
	 * The timer achieves faster performance by using a low-resolution approach,
	 * with an accuracy target of within 500ms. This makes it particularly useful
	 * for timers with delays of 1 second or more, where exact timing is less
	 * crucial.
	 *
	 * It's important to note that Node.js timers are inherently imprecise, as
	 * delays can occur due to the event loop being blocked by other operations.
	 * Consequently, timers may trigger later than their scheduled time.
	 */
/**
	 * The fastNow variable contains the internal fast timer clock value.
	 *
	 * @type {number}
	 */
let A=0;
/**
	 * RESOLUTION_MS represents the target resolution time in milliseconds.
	 *
	 * @type {number}
	 * @default 1000
	 */const e=1e3;
/**
	 * TICK_MS defines the desired interval in milliseconds between each tick.
	 * The target value is set to half the resolution time, minus 1 ms, to account
	 * for potential event loop overhead.
	 *
	 * @type {number}
	 * @default 499
	 */
/**
	 * fastNowTimeout is a Node.js timer used to manage and process
	 * the FastTimers stored in the `fastTimers` array.
	 *
	 * @type {NodeJS.Timeout}
	 */
let t;
/**
	 * The kFastTimer symbol is used to identify FastTimer instances.
	 *
	 * @type {Symbol}
	 */const r=Symbol("kFastTimer"),n=[];
/**
	 * The fastTimers array contains all active FastTimers.
	 *
	 * @type {FastTimer[]}
	 */
/**
	 * The onTick function processes the fastTimers array.
	 *
	 * @returns {void}
	 */
function s(){
/**
	   * Increment the fastNow value by the TICK_MS value, despite the actual time
	   * that has passed since the last tick. This approach ensures independence
	   * from the system clock and delays caused by a blocked event loop.
	   *
	   * @type {number}
	   */
A+=499;
/**
	   * The `idx` variable is used to iterate over the `fastTimers` array.
	   * Expired timers are removed by replacing them with the last element in the array.
	   * Consequently, `idx` is only incremented when the current element is not removed.
	   *
	   * @type {number}
	   */
let e=0,t=n.length;
/**
	   * The len variable will contain the length of the fastTimers array
	   * and will be decremented when a FastTimer should be removed from the
	   * fastTimers array.
	   *
	   * @type {number}
	   */for(;e<t;){
/**
	     * @type {FastTimer}
	     */
const r=n[e];
// If the timer is in the ACTIVE state and the timer has expired, it will
// be processed in the next tick.
0===r._state?(
// Set the _idleStart value to the fastNow value minus the TICK_MS value
// to account for the time the timer was in the PENDING state.
r._idleStart=A-499,r._state=1):1===r._state&&A>=r._idleStart+r._idleTimeout&&(r._state=-1,r._idleStart=-1,r._onTimeout(r._timerArg)),-1===r._state?(r._state=-2,
// Move the last element to the current index and decrement len if it is
// not the only element in the array.
0!==--t&&(n[e]=n[t])):++e}
// Set the length of the fastTimers array to the new length and thus
// removing the excess FastTimers elements from the array.
n.length=t,
// If there are still active FastTimers in the array, refresh the Timer.
// If there are no active FastTimers, the timer will be refreshed again
// when a new FastTimer is instantiated.
0!==n.length&&o()}function o(){
// If the fastNowTimeout is already set, refresh it.
t?t.refresh():(clearTimeout(t),t=setTimeout(s,499),
// If the Timer has an unref method, call it to allow the process to exit if
// there are no other active handles.
t.unref&&t.unref())}
/**
	 * The `FastTimer` class is a data structure designed to store and manage
	 * timer information.
	 */class i{[r]=!0
/**
	   * The state of the timer, which can be one of the following:
	   * - NOT_IN_LIST (-2)
	   * - TO_BE_CLEARED (-1)
	   * - PENDING (0)
	   * - ACTIVE (1)
	   *
	   * @type {-2|-1|0|1}
	   * @private
	   */;_state=-2;
/**
	   * The number of milliseconds to wait before calling the callback.
	   *
	   * @type {number}
	   * @private
	   */_idleTimeout=-1
/**
	   * The time in milliseconds when the timer was started. This value is used to
	   * calculate when the timer should expire.
	   *
	   * @type {number}
	   * @default -1
	   * @private
	   */;_idleStart=-1
/**
	   * The function to be executed when the timer expires.
	   * @type {Function}
	   * @private
	   */;_onTimeout;
/**
	   * The argument to be passed to the callback when the timer expires.
	   *
	   * @type {*}
	   * @private
	   */_timerArg;
/**
	   * @constructor
	   * @param {Function} callback A function to be executed after the timer
	   * expires.
	   * @param {number} delay The time, in milliseconds that the timer should wait
	   * before the specified function or code is executed.
	   * @param {*} arg
	   */constructor(A,e,t){this._onTimeout=A,this._idleTimeout=e,this._timerArg=t,this.refresh()}
/**
	   * Sets the timer's start time to the current time, and reschedules the timer
	   * to call its callback at the previously specified duration adjusted to the
	   * current time.
	   * Using this on a timer that has already called its callback will reactivate
	   * the timer.
	   *
	   * @returns {void}
	   */refresh(){
// In the special case that the timer is not in the list of active timers,
// add it back to the array to be processed in the next tick by the onTick
// function.
-2===this._state&&n.push(this),
// If the timer is the only active timer, refresh the fastNowTimeout for
// better resolution.
t&&1!==n.length||o(),
// Setting the state to PENDING will cause the timer to be reset in the
// next tick by the onTick function.
this._state=0}
/**
	   * The `clear` method cancels the timer, preventing it from executing.
	   *
	   * @returns {void}
	   * @private
	   */clear(){
// Set the state to TO_BE_CLEARED to mark the timer for removal in the next
// tick by the onTick function.
this._state=-1,
// Reset the _idleStart value to -1 to indicate that the timer is no longer
// active.
this._idleStart=-1}}
/**
	 * This module exports a setTimeout and clearTimeout function that can be
	 * used as a drop-in replacement for the native functions.
	 */return aA={
/**
	   * The setTimeout() method sets a timer which executes a function once the
	   * timer expires.
	   * @param {Function} callback A function to be executed after the timer
	   * expires.
	   * @param {number} delay The time, in milliseconds that the timer should
	   * wait before the specified function or code is executed.
	   * @param {*} [arg] An optional argument to be passed to the callback function
	   * when the timer expires.
	   * @returns {NodeJS.Timeout|FastTimer}
	   */
setTimeout:(A,t,r)=>t<=e?setTimeout(A,t,r):new i(A,t,r),
/**
	   * The clearTimeout method cancels an instantiated Timer previously created
	   * by calling setTimeout.
	   *
	   * @param {NodeJS.Timeout|FastTimer} timeout
	   */
clearTimeout(A){
// If the timeout is a FastTimer, call its own clear method.
A[r]?
/**
	       * @type {FastTimer}
	       */
A.clear():clearTimeout(A)},
/**
	   * The setFastTimeout() method sets a fastTimer which executes a function once
	   * the timer expires.
	   * @param {Function} callback A function to be executed after the timer
	   * expires.
	   * @param {number} delay The time, in milliseconds that the timer should
	   * wait before the specified function or code is executed.
	   * @param {*} [arg] An optional argument to be passed to the callback function
	   * when the timer expires.
	   * @returns {FastTimer}
	   */
setFastTimeout:(A,e,t)=>new i(A,e,t),
/**
	   * The clearTimeout method cancels an instantiated FastTimer previously
	   * created by calling setFastTimeout.
	   *
	   * @param {FastTimer} timeout
	   */
clearFastTimeout(A){A.clear()},
/**
	   * The now method returns the value of the internal fast timer clock.
	   *
	   * @returns {number}
	   */
now:()=>A,
/**
	   * Trigger the onTick function to process the fastTimers array.
	   * Exported for testing purposes only.
	   * Marking as deprecated to discourage any use outside of testing.
	   * @deprecated
	   * @param {number} [delay=0] The delay in milliseconds to add to the now value.
	   */
tick(t=0){A+=t-e+1,s(),s()},
/**
	   * Reset FastTimers.
	   * Exported for testing purposes only.
	   * Marking as deprecated to discourage any use outside of testing.
	   * @deprecated
	   */
reset(){A=0,n.length=0,clearTimeout(t),t=null},
/**
	   * Exporting for testing purposes only.
	   * Marking as deprecated to discourage any use outside of testing.
	   * @deprecated
	   */
kFastTimer:r}}function pA(){if(QA)return cA;QA=1;const A=B,e=E,t=CA(),{InvalidArgumentError:r,ConnectTimeoutError:n}=hA(),s=fA();function o(){}let i,a;// include tls conditionally since it is not always available
// TODO: session re-use does not wait for the first
// connection to resolve the session and might therefore
// resolve the same servername multiple times even when
// re-use is enabled.
// FIXME: remove workaround when the Node bug is fixed
// https://github.com/nodejs/node/issues/49344#issuecomment-1741776308
a=!x.FinalizationRegistry||process.env.NODE_V8_COVERAGE||process.env.UNDICI_NO_FG?class{constructor(A){this._maxCachedSessions=A,this._sessionCache=new Map}get(A){return this._sessionCache.get(A)}set(A,e){if(0!==this._maxCachedSessions){if(this._sessionCache.size>=this._maxCachedSessions){
// remove the oldest session
const{value:A}=this._sessionCache.keys().next();this._sessionCache.delete(A)}this._sessionCache.set(A,e)}}}:class{constructor(A){this._maxCachedSessions=A,this._sessionCache=new Map,this._sessionRegistry=new x.FinalizationRegistry(A=>{if(this._sessionCache.size<this._maxCachedSessions)return;const e=this._sessionCache.get(A);void 0!==e&&void 0===e.deref()&&this._sessionCache.delete(A)})}get(A){const e=this._sessionCache.get(A);return e?e.deref():null}set(A,e){0!==this._maxCachedSessions&&(this._sessionCache.set(A,new WeakRef(e)),this._sessionRegistry.register(e,A))}};
/**
	 * @param {WeakRef<net.Socket>} socketWeakRef
	 * @param {object} opts
	 * @param {number} opts.timeout
	 * @param {string} opts.hostname
	 * @param {number} opts.port
	 * @returns {() => void}
	 */
const g="win32"===process.platform?(A,e)=>{if(!e.timeout)return o;let t=null,r=null;const n=s.setFastTimeout(()=>{
// setImmediate is added to make sure that we prioritize socket error events over timeouts
t=setImmediate(()=>{
// Windows needs an extra setImmediate probably due to implementation differences in the socket logic
r=setImmediate(()=>c(A.deref(),e))})},e.timeout);return()=>{s.clearFastTimeout(n),clearImmediate(t),clearImmediate(r)}}:(A,e)=>{if(!e.timeout)return o;let t=null;const r=s.setFastTimeout(()=>{
// setImmediate is added to make sure that we prioritize socket error events over timeouts
t=setImmediate(()=>{c(A.deref(),e)})},e.timeout);return()=>{s.clearFastTimeout(r),clearImmediate(t)}};
/**
	 * @param {net.Socket} socket
	 * @param {object} opts
	 * @param {number} opts.timeout
	 * @param {string} opts.hostname
	 * @param {number} opts.port
	 */function c(A,e){
// The socket could be already garbage collected
if(null==A)return;let r="Connect Timeout Error";Array.isArray(A.autoSelectFamilyAttemptedAddresses)?r+=` (attempted addresses: ${A.autoSelectFamilyAttemptedAddresses.join(", ")},`:r+=` (attempted address: ${e.hostname}:${e.port},`,r+=` timeout: ${e.timeout}ms)`,t.destroy(A,new n(r))}return cA=function({allowH2:n,maxCachedSessions:s,socketPath:o,timeout:c,session:Q,...E}){if(null!=s&&(!Number.isInteger(s)||s<0))throw new r("maxCachedSessions must be a positive integer or zero");const B={path:o,...E},h=new a(s??100);return c=c??1e4,n=null!=n&&n,function({hostname:r,host:s,protocol:o,port:a,servername:E,localAddress:I,httpSocket:C},l){let u;if("https:"===o){i||(i=p);const A=(E=E||B.servername||t.getServerName(s)||null)||r;e(A);const o=Q||h.get(A)||null;a=a||443,u=i.connect({highWaterMark:16384,// TLS in node can't have bigger HWM anyway...
...B,servername:E,session:o,localAddress:I,
// TODO(HTTP/2): Add support for h2c
ALPNProtocols:n?["http/1.1","h2"]:["http/1.1"],socket:C,// upgrade socket connection
port:a,host:r}),u.on("session",function(e){
// TODO (fix): Can a session become invalid once established? Don't think so?
h.set(A,e)})}else e(!C,"httpSocket can only be sent on TLS update"),a=a||80,u=A.connect({highWaterMark:65536,// Same as nodejs fs streams.
...B,localAddress:I,port:a,host:r});
// Set TCP keep alive options on the socket here instead of in connect() for the case of assigning the socket
if(null==B.keepAlive||B.keepAlive){const A=void 0===B.keepAliveInitialDelay?6e4:B.keepAliveInitialDelay;u.setKeepAlive(!0,A)}const d=g(new WeakRef(u),{timeout:c,hostname:r,port:a});return u.setNoDelay(!0).once("https:"===o?"secureConnect":"connect",function(){if(queueMicrotask(d),l){const A=l;l=null,A(null,this)}}).on("error",function(A){if(queueMicrotask(d),l){const e=l;l=null,e(A)}}),u}}}var wA,yA,DA,mA,kA,RA,bA,FA,SA,NA,MA,UA,LA,TA,GA,YA,vA,JA,HA,VA,xA,WA,OA,PA,qA,ZA,zA,KA,XA,_A,jA,$A,Ae,ee,te,re,ne,se,oe,ie,ae,ge,ce,Qe,Ee,Be,he,Ie,Ce,le,ue,de,fe,pe,we,ye,De={},me={};function ke(){return yA||(yA=1,function(A){Object.defineProperty(A,"__esModule",{value:!0}),A.SPECIAL_HEADERS=A.HEADER_STATE=A.MINOR=A.MAJOR=A.CONNECTION_TOKEN_CHARS=A.HEADER_CHARS=A.TOKEN=A.STRICT_TOKEN=A.HEX=A.URL_CHAR=A.STRICT_URL_CHAR=A.USERINFO_CHARS=A.MARK=A.ALPHANUM=A.NUM=A.HEX_MAP=A.NUM_MAP=A.ALPHA=A.FINISH=A.H_METHOD_MAP=A.METHOD_MAP=A.METHODS_RTSP=A.METHODS_ICE=A.METHODS_HTTP=A.METHODS=A.LENIENT_FLAGS=A.FLAGS=A.TYPE=A.ERROR=void 0;const e=(wA||(wA=1,Object.defineProperty(me,"__esModule",{value:!0}),me.enumToMap=void 0,me.enumToMap=function(A){const e={};return Object.keys(A).forEach(t=>{const r=A[t];"number"==typeof r&&(e[t]=r)}),e}),me);var t,r,n,s,o,i,a;(t=A.ERROR||(A.ERROR={}))[t.OK=0]="OK",t[t.INTERNAL=1]="INTERNAL",t[t.STRICT=2]="STRICT",t[t.LF_EXPECTED=3]="LF_EXPECTED",t[t.UNEXPECTED_CONTENT_LENGTH=4]="UNEXPECTED_CONTENT_LENGTH",t[t.CLOSED_CONNECTION=5]="CLOSED_CONNECTION",t[t.INVALID_METHOD=6]="INVALID_METHOD",t[t.INVALID_URL=7]="INVALID_URL",t[t.INVALID_CONSTANT=8]="INVALID_CONSTANT",t[t.INVALID_VERSION=9]="INVALID_VERSION",t[t.INVALID_HEADER_TOKEN=10]="INVALID_HEADER_TOKEN",t[t.INVALID_CONTENT_LENGTH=11]="INVALID_CONTENT_LENGTH",t[t.INVALID_CHUNK_SIZE=12]="INVALID_CHUNK_SIZE",t[t.INVALID_STATUS=13]="INVALID_STATUS",t[t.INVALID_EOF_STATE=14]="INVALID_EOF_STATE",t[t.INVALID_TRANSFER_ENCODING=15]="INVALID_TRANSFER_ENCODING",t[t.CB_MESSAGE_BEGIN=16]="CB_MESSAGE_BEGIN",t[t.CB_HEADERS_COMPLETE=17]="CB_HEADERS_COMPLETE",t[t.CB_MESSAGE_COMPLETE=18]="CB_MESSAGE_COMPLETE",t[t.CB_CHUNK_HEADER=19]="CB_CHUNK_HEADER",t[t.CB_CHUNK_COMPLETE=20]="CB_CHUNK_COMPLETE",t[t.PAUSED=21]="PAUSED",t[t.PAUSED_UPGRADE=22]="PAUSED_UPGRADE",t[t.PAUSED_H2_UPGRADE=23]="PAUSED_H2_UPGRADE",t[t.USER=24]="USER",(r=A.TYPE||(A.TYPE={}))[r.BOTH=0]="BOTH",r[r.REQUEST=1]="REQUEST",r[r.RESPONSE=2]="RESPONSE",(n=A.FLAGS||(A.FLAGS={}))[n.CONNECTION_KEEP_ALIVE=1]="CONNECTION_KEEP_ALIVE",n[n.CONNECTION_CLOSE=2]="CONNECTION_CLOSE",n[n.CONNECTION_UPGRADE=4]="CONNECTION_UPGRADE",n[n.CHUNKED=8]="CHUNKED",n[n.UPGRADE=16]="UPGRADE",n[n.CONTENT_LENGTH=32]="CONTENT_LENGTH",n[n.SKIPBODY=64]="SKIPBODY",n[n.TRAILING=128]="TRAILING",
// 1 << 8 is unused
n[n.TRANSFER_ENCODING=512]="TRANSFER_ENCODING",(s=A.LENIENT_FLAGS||(A.LENIENT_FLAGS={}))[s.HEADERS=1]="HEADERS",s[s.CHUNKED_LENGTH=2]="CHUNKED_LENGTH",s[s.KEEP_ALIVE=4]="KEEP_ALIVE",function(A){A[A.DELETE=0]="DELETE",A[A.GET=1]="GET",A[A.HEAD=2]="HEAD",A[A.POST=3]="POST",A[A.PUT=4]="PUT",
/* pathological */
A[A.CONNECT=5]="CONNECT",A[A.OPTIONS=6]="OPTIONS",A[A.TRACE=7]="TRACE",
/* WebDAV */
A[A.COPY=8]="COPY",A[A.LOCK=9]="LOCK",A[A.MKCOL=10]="MKCOL",A[A.MOVE=11]="MOVE",A[A.PROPFIND=12]="PROPFIND",A[A.PROPPATCH=13]="PROPPATCH",A[A.SEARCH=14]="SEARCH",A[A.UNLOCK=15]="UNLOCK",A[A.BIND=16]="BIND",A[A.REBIND=17]="REBIND",A[A.UNBIND=18]="UNBIND",A[A.ACL=19]="ACL",
/* subversion */
A[A.REPORT=20]="REPORT",A[A.MKACTIVITY=21]="MKACTIVITY",A[A.CHECKOUT=22]="CHECKOUT",A[A.MERGE=23]="MERGE",
/* upnp */
A[A["M-SEARCH"]=24]="M-SEARCH",A[A.NOTIFY=25]="NOTIFY",A[A.SUBSCRIBE=26]="SUBSCRIBE",A[A.UNSUBSCRIBE=27]="UNSUBSCRIBE",
/* RFC-5789 */
A[A.PATCH=28]="PATCH",A[A.PURGE=29]="PURGE",
/* CalDAV */
A[A.MKCALENDAR=30]="MKCALENDAR",
/* RFC-2068, section 19.6.1.2 */
A[A.LINK=31]="LINK",A[A.UNLINK=32]="UNLINK",
/* icecast */
A[A.SOURCE=33]="SOURCE",
/* RFC-7540, section 11.6 */
A[A.PRI=34]="PRI",
/* RFC-2326 RTSP */
A[A.DESCRIBE=35]="DESCRIBE",A[A.ANNOUNCE=36]="ANNOUNCE",A[A.SETUP=37]="SETUP",A[A.PLAY=38]="PLAY",A[A.PAUSE=39]="PAUSE",A[A.TEARDOWN=40]="TEARDOWN",A[A.GET_PARAMETER=41]="GET_PARAMETER",A[A.SET_PARAMETER=42]="SET_PARAMETER",A[A.REDIRECT=43]="REDIRECT",A[A.RECORD=44]="RECORD",
/* RAOP */
A[A.FLUSH=45]="FLUSH"}(o=A.METHODS||(A.METHODS={})),A.METHODS_HTTP=[o.DELETE,o.GET,o.HEAD,o.POST,o.PUT,o.CONNECT,o.OPTIONS,o.TRACE,o.COPY,o.LOCK,o.MKCOL,o.MOVE,o.PROPFIND,o.PROPPATCH,o.SEARCH,o.UNLOCK,o.BIND,o.REBIND,o.UNBIND,o.ACL,o.REPORT,o.MKACTIVITY,o.CHECKOUT,o.MERGE,o["M-SEARCH"],o.NOTIFY,o.SUBSCRIBE,o.UNSUBSCRIBE,o.PATCH,o.PURGE,o.MKCALENDAR,o.LINK,o.UNLINK,o.PRI,
// TODO(indutny): should we allow it with HTTP?
o.SOURCE],A.METHODS_ICE=[o.SOURCE],A.METHODS_RTSP=[o.OPTIONS,o.DESCRIBE,o.ANNOUNCE,o.SETUP,o.PLAY,o.PAUSE,o.TEARDOWN,o.GET_PARAMETER,o.SET_PARAMETER,o.REDIRECT,o.RECORD,o.FLUSH,
// For AirPlay
o.GET,o.POST],A.METHOD_MAP=e.enumToMap(o),A.H_METHOD_MAP={},Object.keys(A.METHOD_MAP).forEach(e=>{/^H/.test(e)&&(A.H_METHOD_MAP[e]=A.METHOD_MAP[e])}),(i=A.FINISH||(A.FINISH={}))[i.SAFE=0]="SAFE",i[i.SAFE_WITH_CB=1]="SAFE_WITH_CB",i[i.UNSAFE=2]="UNSAFE",A.ALPHA=[];for(let e="A".charCodeAt(0);e<="Z".charCodeAt(0);e++)
// Upper case
A.ALPHA.push(String.fromCharCode(e)),
// Lower case
A.ALPHA.push(String.fromCharCode(e+32));A.NUM_MAP={0:0,1:1,2:2,3:3,4:4,5:5,6:6,7:7,8:8,9:9},A.HEX_MAP={0:0,1:1,2:2,3:3,4:4,5:5,6:6,7:7,8:8,9:9,A:10,B:11,C:12,D:13,E:14,F:15,a:10,b:11,c:12,d:13,e:14,f:15},A.NUM=["0","1","2","3","4","5","6","7","8","9"],A.ALPHANUM=A.ALPHA.concat(A.NUM),A.MARK=["-","_",".","!","~","*","'","(",")"],A.USERINFO_CHARS=A.ALPHANUM.concat(A.MARK).concat(["%",";",":","&","=","+","$",","]),
// TODO(indutny): use RFC
A.STRICT_URL_CHAR=["!",'"',"$","%","&","'","(",")","*","+",",","-",".","/",":",";","<","=",">","@","[","\\","]","^","_","`","{","|","}","~"].concat(A.ALPHANUM),A.URL_CHAR=A.STRICT_URL_CHAR.concat(["\t","\f"]);
// All characters with 0x80 bit set to 1
for(let e=128;e<=255;e++)A.URL_CHAR.push(e);A.HEX=A.NUM.concat(["a","b","c","d","e","f","A","B","C","D","E","F"]),
/* Tokens as defined by rfc 2616. Also lowercases them.
		 *        token       = 1*<any CHAR except CTLs or separators>
		 *     separators     = "(" | ")" | "<" | ">" | "@"
		 *                    | "," | ";" | ":" | "\" | <">
		 *                    | "/" | "[" | "]" | "?" | "="
		 *                    | "{" | "}" | SP | HT
		 */
A.STRICT_TOKEN=["!","#","$","%","&","'","*","+","-",".","^","_","`","|","~"].concat(A.ALPHANUM),A.TOKEN=A.STRICT_TOKEN.concat([" "]),
/*
		 * Verify that a char is a valid visible (printable) US-ASCII
		 * character or %x80-FF
		 */
A.HEADER_CHARS=["\t"];for(let e=32;e<=255;e++)127!==e&&A.HEADER_CHARS.push(e);
// ',' = \x44
A.CONNECTION_TOKEN_CHARS=A.HEADER_CHARS.filter(A=>44!==A),A.MAJOR=A.NUM_MAP,A.MINOR=A.MAJOR,function(A){A[A.GENERAL=0]="GENERAL",A[A.CONNECTION=1]="CONNECTION",A[A.CONTENT_LENGTH=2]="CONTENT_LENGTH",A[A.TRANSFER_ENCODING=3]="TRANSFER_ENCODING",A[A.UPGRADE=4]="UPGRADE",A[A.CONNECTION_KEEP_ALIVE=5]="CONNECTION_KEEP_ALIVE",A[A.CONNECTION_CLOSE=6]="CONNECTION_CLOSE",A[A.CONNECTION_UPGRADE=7]="CONNECTION_UPGRADE",A[A.TRANSFER_ENCODING_CHUNKED=8]="TRANSFER_ENCODING_CHUNKED"}(a=A.HEADER_STATE||(A.HEADER_STATE={})),A.SPECIAL_HEADERS={connection:a.CONNECTION,"content-length":a.CONTENT_LENGTH,"proxy-connection":a.CONNECTION,"transfer-encoding":a.TRANSFER_ENCODING,upgrade:a.UPGRADE}}(De)),De}function Re(){if(mA)return DA;mA=1;const{Buffer:A}=C;return DA=A.from("AGFzbQEAAAABJwdgAX8Bf2ADf39/AX9gAX8AYAJ/fwBgBH9/f38Bf2AAAGADf39/AALLAQgDZW52GHdhc21fb25faGVhZGVyc19jb21wbGV0ZQAEA2VudhV3YXNtX29uX21lc3NhZ2VfYmVnaW4AAANlbnYLd2FzbV9vbl91cmwAAQNlbnYOd2FzbV9vbl9zdGF0dXMAAQNlbnYUd2FzbV9vbl9oZWFkZXJfZmllbGQAAQNlbnYUd2FzbV9vbl9oZWFkZXJfdmFsdWUAAQNlbnYMd2FzbV9vbl9ib2R5AAEDZW52GHdhc21fb25fbWVzc2FnZV9jb21wbGV0ZQAAAy0sBQYAAAIAAAAAAAACAQIAAgICAAADAAAAAAMDAwMBAQEBAQEBAQEAAAIAAAAEBQFwARISBQMBAAIGCAF/AUGA1AQLB9EFIgZtZW1vcnkCAAtfaW5pdGlhbGl6ZQAIGV9faW5kaXJlY3RfZnVuY3Rpb25fdGFibGUBAAtsbGh0dHBfaW5pdAAJGGxsaHR0cF9zaG91bGRfa2VlcF9hbGl2ZQAvDGxsaHR0cF9hbGxvYwALBm1hbGxvYwAxC2xsaHR0cF9mcmVlAAwEZnJlZQAMD2xsaHR0cF9nZXRfdHlwZQANFWxsaHR0cF9nZXRfaHR0cF9tYWpvcgAOFWxsaHR0cF9nZXRfaHR0cF9taW5vcgAPEWxsaHR0cF9nZXRfbWV0aG9kABAWbGxodHRwX2dldF9zdGF0dXNfY29kZQAREmxsaHR0cF9nZXRfdXBncmFkZQASDGxsaHR0cF9yZXNldAATDmxsaHR0cF9leGVjdXRlABQUbGxodHRwX3NldHRpbmdzX2luaXQAFQ1sbGh0dHBfZmluaXNoABYMbGxodHRwX3BhdXNlABcNbGxodHRwX3Jlc3VtZQAYG2xsaHR0cF9yZXN1bWVfYWZ0ZXJfdXBncmFkZQAZEGxsaHR0cF9nZXRfZXJybm8AGhdsbGh0dHBfZ2V0X2Vycm9yX3JlYXNvbgAbF2xsaHR0cF9zZXRfZXJyb3JfcmVhc29uABwUbGxodHRwX2dldF9lcnJvcl9wb3MAHRFsbGh0dHBfZXJybm9fbmFtZQAeEmxsaHR0cF9tZXRob2RfbmFtZQAfEmxsaHR0cF9zdGF0dXNfbmFtZQAgGmxsaHR0cF9zZXRfbGVu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PgtBhwEMPQtBiAEMPAtBiQEMOwtBigEMOgtBiwEMOQtBjAEMOAtBjQEMNwtBjgEMNgtBjwEMNQtBkAEMNAtBkQEMMwtBkgEMMgtBkwEMMQtBlAEMMAtBlQEMLwtBlgEMLgtBlwEMLQtBmAEMLAtBmQEMKwtBmgEMKgtBmwEMKQtBnAEMKAtBnQEMJwtBngEMJgtBnwEMJQtBoAEMJAtBoQEMIwtBogEMIgtBowEMIQtBpAEMIAtBpQEMHwtBpgEMHgtBpwEMHQtBqAEMHAtBqQEMGwtBqgEMGgtBqwEMGQtBrAEMGAtBrQEMFwtBrgEMFgtBAQwVC0GvAQwUC0GwAQwTC0GxAQwSC0GzAQwRC0GyAQwQC0G0AQwPC0G1AQwOC0G2AQwNC0G3AQwMC0G4AQwLC0G5AQwKC0G6AQwJC0G7AQwIC0HGAQwHC0G8AQwGC0G9AQwFC0G+AQwEC0G/AQwDC0HAAQwCC0HCAQwBC0HBAQshAwNAAkACQAJAAkACQAJAAkACQAJAIAICfwJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJ/AkACQAJAAkACQAJAAkACQAJAAkACQAJAAkAgAgJ/AkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACfwJAAkACfwJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkAC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iQEhAwykAgtBASABLQAAQd8ARw1nGiABQQFqIQEMXgtBigEhAyABIARGDaICIAIoAgAiACAEIAFraiEFIAEgAGtBAWohBgNAIAEtAAAgAEGMzwBqLQAARw1kIABBAUYN+gEgAEEBaiEAIAQgAUEBaiIBRw0ACyACIAU2AgAMogILQYsBIQMgASAERg2hAiACKAIAIgAgBCABa2ohBSABIABrQQJqIQYCQANAIAEtAAAgAEGOzwBqLQAARw1kIABBAkYNASAAQQFqIQAgBCABQQFqIgFHDQALIAIgBTYCAAyiAgsgAkEANgIAIAZBAWohAUECDGULQYwBIQMgASAERg2gAiACKAIAIgAgBCABa2ohBSABIABrQQFqIQYCQANAIAEtAAAgAEHwzwBqLQAARw1jIABBAUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAIgBTYCAAyhAgsgAkEANgIAIAZBAWohAUEfDGQLQY0BIQMgASAERg2fAiACKAIAIgAgBCABa2ohBSABIABrQQFqIQYCQANAIAEtAAAgAEHyzwBqLQAARw1iIABBAUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAIgBTYCAAygAgsgAkEANgIAIAZBAWohAUEJDGMLIAEgBEYEQEGOASEDDJ8CCwJAAkAgAS0AAEHJAGsOBwBiYmJiYgFiCyABQQFqIQFB+AAhAwyGAgsgAUEBaiEBQfkAIQMMhQILQY8BIQMgASAERg2dAiACKAIAIgAgBCABa2ohBSABIABrQQVqIQYCQANAIAEtAAAgAEGRzwBqLQAARw1gIABBBUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAIgBTYCAAyeAgsgAkEANgIAIAZBAWohAUEYDGELQZABIQMgASAERg2cAiACKAIAIgAgBCABa2ohBSABIABrQQJqIQYCQANAIAEtAAAgAEGXzwBqLQAARw1fIABBAkYNASAAQQFqIQAgBCABQQFqIgFHDQALIAIgBTYCAAydAgsgAkEANgIAIAZBAWohAUEXDGALQZEBIQMgASAERg2bAiACKAIAIgAgBCABa2ohBSABIABrQQZqIQYCQANAIAEtAAAgAEGazwBqLQAARw1eIABBBkYNASAAQQFqIQAgBCABQQFqIgFHDQALIAIgBTYCAAycAgsgAkEANgIAIAZBAWohAUEV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JSIARQ0RIAJB5QA2AhwgAiABNgIUIAIgADYCDEEAIQMMtAELIAIoAgQhACACQQA2AgQgAiAAIAEQJSIARQ0gIAJB0wA2AhwgAiABNgIUIAIgADYCDEEAIQMMswELIAIoAgQhACACQQA2AgQgAiAAIAEQJSIARQ0iIAJB0gA2AhwgAiABNgIUIAIgADYCDEEAIQMMsgELIAIoAgQhACACQQA2AgQgAiAAIAEQJSIARQ0OIAJB5QA2AhwgAiABNgIUIAIgADYCDEEAIQMMsQELIAIoAgQhACACQQA2AgQgAiAAIAEQJSIARQ0dIAJB0wA2AhwgAiABNgIUIAIgADYCDEEAIQMMsAELIAIoAgQhACACQQA2AgQgAiAAIAEQJSIARQ0fIAJB0gA2AhwgAiABNgIUIAIgADYCDEEAIQMMrwELIABBP0cNASABQQFqCyEBQQUhAwyUAQtBACEDIAJBADYCHCACIAE2AhQgAkH9EjYCECACQQc2AgwMrAELIAJBADYCHCACIAE2AhQgAkHcCDYCECACQQc2AgxBACEDDKsBCyACKAIEIQAgAkEANgIEIAIgACABECUiAEUNByACQeUANgIcIAIgATYCFCACIAA2AgxBACEDDKoBCyACKAIEIQAgAkEANgIEIAIgACABECUiAEUNFiACQdMANgIcIAIgATYCFCACIAA2AgxBACEDDKkBCyACKAIEIQAgAkEANgIEIAIgACABECUiAEUNGCACQdIANgIcIAIgATYCFCACIAA2AgxBACEDDKgBCyACQQA2AhwgAiABNgIUIAJBxgo2AhAgAkEHNgIMQQAhAwynAQsgAigCBCEAIAJBADYCBCACIAAgARAlIgBFDQMgAkHlADYCHCACIAE2AhQgAiAANgIMQQAhAwymAQsgAigCBCEAIAJBADYCBCACIAAgARAlIgBFDRIgAkHTADYCHCACIAE2AhQgAiAANgIMQQAhAwylAQsgAigCBCEAIAJBADYCBCACIAAgARAlIgBFDRQgAkHSADYCHCACIAE2AhQgAiAANgIMQQAhAwykAQsgAigCBCEAIAJBADYCBCACIAAgARAlIgBFDQAgAkHlADYCHCACIAE2AhQgAiAANgIMQQAhAwyjAQtB1QAhAwyJAQsgAEEVRwRAIAJBADYCHCACIAE2AhQgAkG5DTYCECACQRo2AgxB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AkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAIABB5ABrDvQDY2IAAWFhYWFhYQIDBAVhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhBgcICQoLDA0OD2FhYWFhEGFhYWFhYWFhYWFhEWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYRITFBUWFxgZGhthYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhHB0eHyAhIiMkJSYnKCkqKywtLi8wMTIzNDU2YTc4OTphYWFhYWFhYTthYWE8YWFhYT0+P2FhYWFhYWFhQGFhQWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYUJDREVGR0hJSktMTU5PUFFSU2FhYWFhYWFhVFVWV1hZWlthXF1hYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFeYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhX2BhC0HhJw8LQaQhDwtByywPC0H+MQ8LQcAkDwtBqyQPC0GNKA8LQeImDwtBgDAPC0G5Lw8LQdckDwtB7x8PC0HhHw8LQfofDwtB8iAPC0GoLw8LQa4yDwtBiDAPC0HsJw8LQYIiDwtBjh0PC0HQLg8LQcojDwtBxTIPC0HfHA8LQdIcDwtBxCAPC0HXIA8LQaIfDwtB7S4PC0GrMA8LQdQlDwtBzC4PC0H6Lg8LQfwrDwtB0jAPC0HxHQ8LQbsgDwtB9ysPC0GQMQ8LQdcxDwtBoi0PC0HUJw8LQeArDwtBnywPC0HrMQ8LQdUfDwtByjEPC0HeJQ8LQdQeDwtB9BwPC0GnMg8LQbEdDwtBoB0PC0G5MQ8LQbwwDwtBkiEPC0GzJg8LQeksDwtBrB4PC0HUKw8LQfcmDwtBgCYPC0GwIQ8LQf4eDwtBjSMPC0GJLQ8LQfciDwtBoDEPC0GuHw8LQcYlDwtB6B4PC0GTIg8LQcIvDwtBwx0PC0GLLA8LQeEdDwtBjS8PC0HqIQ8LQbQtDwtB0i8PC0HfMg8LQdIyDwtB8DAPC0GpIg8LQfkjDwtBmR4PC0G1LA8LQZswDwtBkjIPC0G2Kw8LQcIiDwtB+DIPC0GeJQ8LQdAiDwtBuh4PC0GBHg8LAAtB1iEhAQsgAQsWACAAIAAtAC1B/gFxIAFBAEdyOgAtCxkAIAAgAC0ALUH9AXEgAUEAR0EBdHI6AC0LGQAgACAALQAtQfsBcSABQQBHQQJ0cjoALQsZACAAIAAtAC1B9wFxIAFBAEdBA3RyOgAtCz4BAn8CQCAAKAI4IgNFDQAgAygCBCIDRQ0AIAAgASACIAFrIAMRAQAiBEF/Rw0AIABBxhE2AhBBGCEECyAECz4BAn8CQCAAKAI4IgNFDQAgAygCCCIDRQ0AIAAgASACIAFrIAMRAQAiBEF/Rw0AIABB9go2AhBBGCEECyAECz4BAn8CQCAAKAI4IgNFDQAgAygCDCIDRQ0AIAAgASACIAFrIAMRAQAiBEF/Rw0AIABB7Ro2AhBBGCEECyAECz4BAn8CQCAAKAI4IgNFDQAgAygCECIDRQ0AIAAgASACIAFrIAMRAQAiBEF/Rw0AIABBlRA2AhBBGCEECyAECz4BAn8CQCAAKAI4IgNFDQAgAygCFCIDRQ0AIAAgASACIAFrIAMRAQAiBEF/Rw0AIABBqhs2AhBBGCEECyAECz4BAn8CQCAAKAI4IgNFDQAgAygCGCIDRQ0AIAAgASACIAFrIAMRAQAiBEF/Rw0AIABB7RM2AhBBGCEECyAECz4BAn8CQCAAKAI4IgNFDQAgAygCKCIDRQ0AIAAgASACIAFrIAMRAQAiBEF/Rw0AIABB9gg2AhBBGCEECyAECz4BAn8CQCAAKAI4IgNFDQAgAygCHCIDRQ0AIAAgASACIAFrIAMRAQAiBEF/Rw0AIABBwhk2AhBBGCEECyAECz4BAn8CQCAAKAI4IgNFDQAgAygCICIDRQ0AIAAgASACIAFrIAMRAQAiBEF/Rw0AIABBlBQ2AhBBGCEECyAEC1kBAn8CQCAALQAoQQFGDQAgAC8BMiIBQeQAa0HkAEkNACABQcwBRg0AIAFBsAJGDQAgAC8BMCIAQcAAcQ0AQQEhAiAAQYgEcUGABEYNACAAQShxRSECCyACC4wBAQJ/AkACQAJAIAAtACpFDQAgAC0AK0UNACAALwEwIgFBAnFFDQEMAgsgAC8BMCIBQQFxRQ0BC0EBIQIgAC0AKEEBRg0AIAAvATIiAEHkAGtB5ABJDQAgAEHMAUYNACAAQbACRg0AIAFBwABxDQBBACECIAFBiARxQYAERg0AIAFBKHFBAEchAgsgAgtXACAAQRhqQgA3AwAgAEIANwMAIABBOGpCADcDACAAQTBqQgA3AwAgAEEoakIANwMAIABBIGpCADcDACAAQRBqQgA3AwAgAEEIakIANwMAIABB3QE2AhwLBgAgABAyC5otAQt/IwBBEGsiCiQAQaTQACgCACIJRQRAQeTTACgCACIFRQRAQfDTAEJ/NwIAQejTAEKAgISAgIDAADcCAEHk0wAgCkEIakFwcUHYqtWqBXMiBTYCAEH40wBBADYCAEHI0wBBADYCAAtBzNMAQYDUBDYCAEGc0ABBgNQENgIAQbDQACAFNgIAQazQAEF/NgIAQdDTAEGArAM2AgADQCABQcjQAGogAUG80ABqIgI2AgAgAiABQbTQAGoiAzYCACABQcDQAGogAzYCACABQdDQAGogAUHE0ABqIgM2AgAgAyACNgIAIAFB2NAAaiABQczQAGoiAjYCACACIAM2AgAgAUHU0ABqIAI2AgAgAUEgaiIBQYACRw0AC0GM1ARBwasDNgIAQajQAEH00wAoAgA2AgBBmNAAQcCrAzYCAEGk0ABBiNQENgIAQcz/B0E4NgIAQYjUBCEJCwJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAIABB7AFNBEBBjNAAKAIAIgZBECAAQRNqQXBxIABBC0kbIgRBA3YiAHYiAUEDcQRAAkAgAUEBcSAAckEBcyICQQN0IgBBtNAAaiIBIABBvNAAaigCACIAKAIIIgNGBEBBjNAAIAZBfiACd3E2AgAMAQsgASADNgIIIAMgATYCDAsgAEEIaiEBIAAgAkEDdCICQQNyNgIEIAAgAmoiACAAKAIEQQFyNgIEDBELQZTQACgCACIIIARPDQEgAQRAAkBBAiAAdCICQQAgAmtyIAEgAHRxaCIAQQN0IgJBtNAAaiIBIAJBvNAAaigCACICKAIIIgNGBEBBjNAAIAZBfiAAd3EiBjYCAAwBCyABIAM2AgggAyABNgIMCyACIARBA3I2AgQgAEEDdCIAIARrIQUgACACaiAFNgIAIAIgBGoiBCAFQQFyNgIEIAgEQCAIQXhxQbTQAGohAEGg0AAoAgAhAwJ/QQEgCEEDdnQiASAGcUUEQEGM0AAgASAGcjYCACAADAELIAAoAggLIgEgAzYCDCAAIAM2AgggAyAANgIMIAMgATYCCAsgAkEIaiEBQaDQACAENgIAQZTQACAFNgIADBELQZDQACgCACILRQ0BIAtoQQJ0QbzSAGooAgAiACgCBEF4cSAEayEFIAAhAgNAAkAgAigCECIBRQRAIAJBFGooAgAiAUUNAQsgASgCBEF4cSAEayIDIAVJIQIgAyAFIAIbIQUgASAAIAIbIQAgASECDAELCyAAKAIYIQkgACgCDCIDIABHBEBBnNAAKAIAGiADIAAoAggiATYCCCABIAM2AgwMEAsgAEEUaiICKAIAIgFFBEAgACgCECIBRQ0DIABBEGohAgsDQCACIQcgASIDQRRqIgIoAgAiAQ0AIANBEGohAiADKAIQIgENAAsgB0EANgIADA8LQX8hBCAAQb9/Sw0AIABBE2oiAUFwcSEEQZDQACgCACIIRQ0AQQAgBGshBQJAAkACQAJ/QQAgBEGAAkkNABpBHyAEQf///wdLDQAaIARBJiABQQh2ZyIAa3ZBAXEgAEEBdGtBPmoLIgZBAnRBvNIAaigCACICRQRAQQAhAUEAIQMMAQtBACEBIARBGSAGQQF2a0EAIAZBH0cbdCEAQQAhAwNAAkAgAigCBEF4cSAEayIHIAVPDQAgAiEDIAciBQ0AQQAhBSACIQEMAwsgASACQRRqKAIAIgcgByACIABBHXZBBHFqQRBqKAIAIgJGGyABIAcbIQEgAEEBdCEAIAINAAsLIAEgA3JFBEBBACEDQQIgBnQiAEEAIABrciAIcSIARQ0DIABoQQJ0QbzSAGooAgAhAQsgAUUNAQsDQCABKAIEQXhxIARrIgIgBUkhACACIAUgABshBSABIAMgABshAyABKAIQIgAEfyAABSABQRRqKAIACyIBDQALCyADRQ0AIAVBlNAAKAIAIARrTw0AIAMoAhghByADIAMoAgwiAEcEQEGc0AAoAgAaIAAgAygCCCIBNgIIIAEgADYCDAwOCyADQRRqIgIoAgAiAUUEQCADKAIQIgFFDQMgA0EQaiECCwNAIAIhBiABIgBBFGoiAigCACIBDQAgAEEQaiECIAAoAhAiAQ0ACyAGQQA2AgAMDQtBlNAAKAIAIgMgBE8EQEGg0AAoAgAhAQJAIAMgBGsiAkEQTwRAIAEgBGoiACACQQFyNgIEIAEgA2ogAjYCACABIARBA3I2AgQMAQsgASADQQNyNgIEIAEgA2oiACAAKAIEQQFyNgIEQQAhAEEAIQILQZTQACACNgIAQaDQACAANgIAIAFBCGohAQwPC0GY0AAoAgAiAyAESwRAIAQgCWoiACADIARrIgFBAXI2AgRBpNAAIAA2AgBBmNAAIAE2AgAgCSAEQQNyNgIEIAlBCGohAQwPC0EAIQEgBAJ/QeTTACgCAARAQezTACgCAAwBC0Hw0wBCfzcCAEHo0wBCgICEgICAwAA3AgBB5NMAIApBDGpBcHFB2KrVqgVzNgIAQfjTAEEANgIAQcjTAEEANgIAQYCABAsiACAEQccAaiIFaiIGQQAgAGsiB3EiAk8EQEH80wBBMDYCAAwPCwJAQcTTACgCACIBRQ0AQbzTACgCACIIIAJqIQAgACABTSAAIAhLcQ0AQQAhAUH80wBBMDYCAAwPC0HI0wAtAABBBHENBAJAAkAgCQRAQczTACEBA0AgASgCACIAIAlNBEAgACABKAIEaiAJSw0DCyABKAIIIgENAAsLQQAQMyIAQX9GDQUgAiEGQejTACgCACIBQQFrIgMgAHEEQCACIABrIAAgA2pBACABa3FqIQYLIAQgBk8NBSAGQf7///8HSw0FQcTTACgCACIDBEBBvNMAKAIAIgcgBmohASABIAdNDQYgASADSw0GCyAGEDMiASAARw0BDAcLIAYgA2sgB3EiBkH+////B0sNBCAGEDMhACAAIAEoAgAgASgCBGpGDQMgACEBCwJAIAYgBEHIAGpPDQAgAUF/Rg0AQezTACgCACIAIAUgBmtqQQAgAGtxIgBB/v///wdLBEAgASEADAcLIAAQM0F/RwRAIAAgBmohBiABIQAMBwtBACAGaxAzGgwECyABIgBBf0cNBQwDC0EAIQMMDAtBACEADAoLIABBf0cNAgtByNMAQcjTACgCAEEEcjYCAAsgAkH+////B0sNASACEDMhAEEAEDMhASAAQX9GDQEgAUF/Rg0BIAAgAU8NASABIABrIgYgBEE4ak0NAQtBvNMAQbzTACgCACAGaiIBNgIAQcDTACgCACABSQRAQcDTACABNgIACwJAAkACQEGk0AAoAgAiAgRAQczTACEBA0AgACABKAIAIgMgASgCBCIFakYNAiABKAIIIgENAAsMAgtBnNAAKAIAIgFBAEcgACABT3FFBEBBnNAAIAA2AgALQQAhAUHQ0wAgBjYCAEHM0wAgADYCAEGs0ABBfzYCAEGw0ABB5NMAKAIANgIAQdjTAEEANgIAA0AgAUHI0ABqIAFBvNAAaiICNgIAIAIgAUG00ABqIgM2AgAgAUHA0ABqIAM2AgAgAUHQ0ABqIAFBxNAAaiIDNgIAIAMgAjYCACABQdjQAGogAUHM0ABqIgI2AgAgAiADNgIAIAFB1NAAaiACNgIAIAFBIGoiAUGAAkcNAAtBeCAAa0EPcSIBIABqIgIgBkE4ayIDIAFrIgFBAXI2AgRBqNAAQfTTACgCADYCAEGY0AAgATYCAEGk0AAgAjYCACAAIANqQTg2AgQMAgsgACACTQ0AIAIgA0kNACABKAIMQQhxDQBBeCACa0EPcSIAIAJqIgNBmNAAKAIAIAZqIgcgAGsiAEEBcjYCBCABIAUgBmo2AgRBqNAAQfTTACgCADYCAEGY0AAgADYCAEGk0AAgAzYCACACIAdqQTg2AgQMAQsgAEGc0AAoAgBJBEBBnNAAIAA2AgALIAAgBmohA0HM0wAhAQJAAkACQANAIAMgASgCAEcEQCABKAIIIgENAQwCCwsgAS0ADEEIcUUNAQtBzNMAIQEDQCABKAIAIgMgAk0EQCADIAEoAgRqIgUgAksNAwsgASgCCCEBDAALAAsgASAANgIAIAEgASgCBCAGajYCBCAAQXggAGtBD3FqIgkgBEEDcjYCBCADQXggA2tBD3FqIgYgBCAJaiIEayEBIAIgBkYEQEGk0AAgBDYCAEGY0ABBmNAAKAIAIAFqIgA2AgAgBCAAQQFyNgIEDAgLQaDQACgCACAGRgRAQaDQACAENgIAQZTQAEGU0AAoAgAgAWoiADYCACAEIABBAXI2AgQgACAEaiAANgIADAgLIAYoAgQiBUEDcUEBRw0GIAVBeHEhCCAFQf8BTQRAIAVBA3YhAyAGKAIIIgAgBigCDCICRgRAQYzQAEGM0AAoAgBBfiADd3E2AgAMBwsgAiAANgIIIAAgAjYCDAwGCyAGKAIYIQcgBiAGKAIMIgBHBEAgACAGKAIIIgI2AgggAiAANgIMDAULIAZBFGoiAigCACIFRQRAIAYoAhAiBUUNBCAGQRBqIQILA0AgAiEDIAUiAEEUaiICKAIAIgUNACAAQRBqIQIgACgCECIFDQALIANBADYCAAwEC0F4IABrQQ9xIgEgAGoiByAGQThrIgMgAWsiAUEBcjYCBCAAIANqQTg2AgQgAiAFQTcgBWtBD3FqQT9rIgMgAyACQRBqSRsiA0EjNgIEQajQAEH00wAoAgA2AgBBmNAAIAE2AgBBpNAAIAc2AgAgA0EQakHU0wApAgA3AgAgA0HM0wApAgA3AghB1NMAIANBCGo2AgBB0NMAIAY2AgBBzNMAIAA2AgBB2NMAQQA2AgAgA0EkaiEBA0AgAUEHNgIAIAUgAUEEaiIBSw0ACyACIANGDQAgAyADKAIEQX5xNgIEIAMgAyACayIFNgIAIAIgBUEBcjYCBCAFQf8BTQRAIAVBeHFBtNAAaiEAAn9BjNAAKAIAIgFBASAFQQN2dCIDcUUEQEGM0AAgASADcjYCACAADAELIAAoAggLIgEgAjYCDCAAIAI2AgggAiAANgIMIAIgATYCCAwBC0EfIQEgBUH///8HTQRAIAVBJiAFQQh2ZyIAa3ZBAXEgAEEBdGtBPmohAQsgAiABNgIcIAJCADcCECABQQJ0QbzSAGohAEGQ0AAoAgAiA0EBIAF0IgZxRQRAIAAgAjYCAEGQ0AAgAyAGcjYCACACIAA2AhggAiACNgIIIAIgAjYCDAwBCyAFQRkgAUEBdmtBACABQR9HG3QhASAAKAIAIQMCQANAIAMiACgCBEF4cSAFRg0BIAFBHXYhAyABQQF0IQEgACADQQRxakEQaiIGKAIAIgMNAAsgBiACNgIAIAIgADYCGCACIAI2AgwgAiACNgIIDAELIAAoAggiASACNgIMIAAgAjYCCCACQQA2AhggAiAANgIMIAIgATYCCAtBmNAAKAIAIgEgBE0NAEGk0AAoAgAiACAEaiICIAEgBGsiAUEBcjYCBEGY0AAgATYCAEGk0AAgAjYCACAAIARBA3I2AgQgAEEIaiEBDAgLQQAhAUH80wBBMDYCAAwHC0EAIQALIAdFDQACQCAGKAIcIgJBAnRBvNIAaiIDKAIAIAZGBEAgAyAANgIAIAANAUGQ0ABBkNAAKAIAQX4gAndxNgIADAILIAdBEEEUIAcoAhAgBkYbaiAANgIAIABFDQELIAAgBzYCGCAGKAIQIgIEQCAAIAI2AhAgAiAANgIYCyAGQRRqKAIAIgJFDQAgAEEUaiACNgIAIAIgADYCGAsgASAIaiEBIAYgCGoiBigCBCEFCyAGIAVBfnE2AgQgASAEaiABNgIAIAQgAUEBcjYCBCABQf8BTQRAIAFBeHFBtNAAaiEAAn9BjNAAKAIAIgJBASABQQN2dCIBcUUEQEGM0AAgASACcjYCACAADAELIAAoAggLIgEgBDYCDCAAIAQ2AgggBCAANgIMIAQgATYCCAwBC0EfIQUgAUH///8HTQRAIAFBJiABQQh2ZyIAa3ZBAXEgAEEBdGtBPmohBQsgBCAFNgIcIARCADcCECAFQQJ0QbzSAGohAEGQ0AAoAgAiAkEBIAV0IgNxRQRAIAAgBDYCAEGQ0AAgAiADcjYCACAEIAA2AhggBCAENgIIIAQgBDYCDAwBCyABQRkgBUEBdmtBACAFQR9HG3QhBSAAKAIAIQACQANAIAAiAigCBEF4cSABRg0BIAVBHXYhACAFQQF0IQUgAiAAQQRxakEQaiIDKAIAIgANAAsgAyAENgIAIAQgAjYCGCAEIAQ2AgwgBCAENgIIDAELIAIoAggiACAENgIMIAIgBDYCCCAEQQA2AhggBCACNgIMIAQgADYCCAsgCUEIaiEBDAILAkAgB0UNAAJAIAMoAhwiAUECdEG80gBqIgIoAgAgA0YEQCACIAA2AgAgAA0BQZDQACAIQX4gAXdxIgg2AgAMAgsgB0EQQRQgBygCECADRhtqIAA2AgAgAEUNAQsgACAHNgIYIAMoAhAiAQRAIAAgATYCECABIAA2AhgLIANBFGooAgAiAUUNACAAQRRqIAE2AgAgASAANgIYCwJAIAVBD00EQCADIAQgBWoiAEEDcjYCBCAAIANqIgAgACgCBEEBcjYCBAwBCyADIARqIgIgBUEBcjYCBCADIARBA3I2AgQgAiAFaiAFNgIAIAVB/wFNBEAgBUF4cUG00ABqIQACf0GM0AAoAgAiAUEBIAVBA3Z0IgVxRQRAQYzQACABIAVyNgIAIAAMAQsgACgCCAsiASACNgIMIAAgAjYCCCACIAA2AgwgAiABNgIIDAELQR8hASAFQf///wdNBEAgBUEmIAVBCHZnIgBrdkEBcSAAQQF0a0E+aiEBCyACIAE2AhwgAkIANwIQIAFBAnRBvNIAaiEAQQEgAXQiBCAIcUUEQCAAIAI2AgBBkNAAIAQgCHI2AgAgAiAANgIYIAIgAjYCCCACIAI2AgwMAQsgBUEZIAFBAXZrQQAgAUEfRxt0IQEgACgCACEEAkADQCAEIgAoAgRBeHEgBUYNASABQR12IQQgAUEBdCEBIAAgBEEEcWpBEGoiBigCACIEDQALIAYgAjYCACACIAA2AhggAiACNgIMIAIgAjYCCAwBCyAAKAIIIgEgAjYCDCAAIAI2AgggAkEANgIYIAIgADYCDCACIAE2AggLIANBCGohAQwBCwJAIAlFDQACQCAAKAIcIgFBAnRBvNIAaiICKAIAIABGBEAgAiADNgIAIAMNAUGQ0AAgC0F+IAF3cTYCAAwCCyAJQRBBFCAJKAIQIABGG2ogAzYCACADRQ0BCyADIAk2AhggACgCECIBBEAgAyABNgIQIAEgAzYCGAsgAEEUaigCACIBRQ0AIANBFGogATYCACABIAM2AhgLAkAgBUEPTQRAIAAgBCAFaiIBQQNyNgIEIAAgAWoiASABKAIEQQFyNgIEDAELIAAgBGoiByAFQQFyNgIEIAAgBEEDcjYCBCAFIAdqIAU2AgAgCARAIAhBeHFBtNAAaiEBQaDQACgCACEDAn9BASAIQQN2dCICIAZxRQRAQYzQACACIAZyNgIAIAEMAQsgASgCCAsiAiADNgIMIAEgAzYCCCADIAE2AgwgAyACNgIIC0Gg0AAgBzYCAEGU0AAgBTYCAAsgAEEIaiEBCyAKQRBqJAAgAQtDACAARQRAPwBBEHQPCwJAIABB//8DcQ0AIABBAEgNACAAQRB2QAAiAEF/RgRAQfzTAEEwNgIAQX8PCyAAQRB0DwsACwvcPyIAQYAICwkBAAAAAgAAAAMAQZQICwUEAAAABQBBpAgLCQYAAAAHAAAACABB3AgLii1JbnZhbGlkIGNoYXIgaW4gdXJsIHF1ZXJ5AFNwYW4gY2FsbGJhY2sgZXJyb3IgaW4gb25fYm9keQBDb250ZW50LUxlbmd0aCBvdmVyZmxvdwBDaHVuayBzaXplIG92ZXJmbG93AFJlc3BvbnNlIG92ZXJmbG93AEludmFsaWQgbWV0aG9kIGZvciBIVFRQL3gueCByZXF1ZXN0AEludmFsaWQgbWV0aG9kIGZvciBSVFNQL3gueCByZXF1ZXN0AEV4cGVjdGVkIFNPVVJDRSBtZXRob2QgZm9yIElDRS94LnggcmVxdWVzdABJbnZhbGlkIGNoYXIgaW4gdXJsIGZyYWdtZW50IHN0YXJ0AEV4cGVjdGVkIGRvdABTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX3N0YXR1cwBJbnZhbGlkIHJlc3BvbnNlIHN0YXR1cwBJbnZhbGlkIGNoYXJhY3RlciBpbiBjaHVuayBleHRlbnNpb25zAFVzZXIgY2FsbGJhY2sgZXJyb3IAYG9uX3Jlc2V0YCBjYWxsYmFjayBlcnJvcgBgb25fY2h1bmtfaGVhZGVyYCBjYWxsYmFjayBlcnJvcgBgb25fbWVzc2FnZV9iZWdpbmAgY2FsbGJhY2sgZXJyb3IAYG9uX2NodW5rX2V4dGVuc2lvbl92YWx1ZWAgY2FsbGJhY2sgZXJyb3IAYG9uX3N0YXR1c19jb21wbGV0ZWAgY2FsbGJhY2sgZXJyb3IAYG9uX3ZlcnNpb25fY29tcGxldGVgIGNhbGxiYWNrIGVycm9yAGBvbl91cmxfY29tcGxldGVgIGNhbGxiYWNrIGVycm9yAGBvbl9jaHVua19jb21wbGV0ZWAgY2FsbGJhY2sgZXJyb3IAYG9uX2hlYWRlcl92YWx1ZV9jb21wbGV0ZWAgY2FsbGJhY2sgZXJyb3IAYG9uX21lc3NhZ2VfY29tcGxldGVgIGNhbGxiYWNrIGVycm9yAGBvbl9tZXRob2RfY29tcGxldGVgIGNhbGxiYWNrIGVycm9yAGBvbl9oZWFkZXJfZmllbGRfY29tcGxldGVgIGNhbGxiYWNrIGVycm9yAGBvbl9jaHVua19leHRlbnNpb25fbmFtZWAgY2FsbGJhY2sgZXJyb3IAVW5leHBlY3RlZCBjaGFyIGluIHVybCBzZXJ2ZXIASW52YWxpZCBoZWFkZXIgdmFsdWUgY2hhcgBJbnZhbGlkIGhlYWRlciBmaWVsZCBjaGFyAFNwYW4gY2FsbGJhY2sgZXJyb3IgaW4gb25fdmVyc2lvbgBJbnZhbGlkIG1pbm9yIHZlcnNpb24ASW52YWxpZCBtYWpvciB2ZXJzaW9uAEV4cGVjdGVkIHNwYWNlIGFmdGVyIHZlcnNpb24ARXhwZWN0ZWQgQ1JMRiBhZnRlciB2ZXJzaW9uAEludmFsaWQgSFRUUCB2ZXJzaW9uAEludmFsaWQgaGVhZGVyIHRva2VuAFNwYW4gY2FsbGJhY2sgZXJyb3IgaW4gb25fdXJsAEludmFsaWQgY2hhcmFjdGVycyBpbiB1cmwAVW5leHBlY3RlZCBzdGFydCBjaGFyIGluIHVybABEb3VibGUgQCBpbiB1cmwARW1wdHkgQ29udGVudC1MZW5ndGgASW52YWxpZCBjaGFyYWN0ZXIgaW4gQ29udGVudC1MZW5ndGgARHVwbGljYXRlIENvbnRlbnQtTGVuZ3RoAEludmFsaWQgY2hhciBpbiB1cmwgcGF0aABDb250ZW50LUxlbmd0aCBjYW4ndCBiZSBwcmVzZW50IHdpdGggVHJhbnNmZXItRW5jb2RpbmcASW52YWxpZCBjaGFyYWN0ZXIgaW4gY2h1bmsgc2l6ZQBTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX2hlYWRlcl92YWx1ZQBTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX2NodW5rX2V4dGVuc2lvbl92YWx1ZQBJbnZhbGlkIGNoYXJhY3RlciBpbiBjaHVuayBleHRlbnNpb25zIHZhbHVlAE1pc3NpbmcgZXhwZWN0ZWQgTEYgYWZ0ZXIgaGVhZGVyIHZhbHVlAEludmFsaWQgYFRyYW5zZmVyLUVuY29kaW5nYCBoZWFkZXIgdmFsdWUASW52YWxpZCBjaGFyYWN0ZXIgaW4gY2h1bmsgZXh0ZW5zaW9ucyBxdW90ZSB2YWx1ZQBJbnZhbGlkIGNoYXJhY3RlciBpbiBjaHVuayBleHRlbnNpb25zIHF1b3RlZCB2YWx1ZQBQYXVzZWQgYnkgb25faGVhZGVyc19jb21wbGV0ZQBJbnZhbGlkIEVPRiBzdGF0ZQBvbl9yZXNldCBwYXVzZQBvbl9jaHVua19oZWFkZXIgcGF1c2UAb25fbWVzc2FnZV9iZWdpbiBwYXVzZQBvbl9jaHVua19leHRlbnNpb25fdmFsdWUgcGF1c2UAb25fc3RhdHVzX2NvbXBsZXRlIHBhdXNlAG9uX3ZlcnNpb25fY29tcGxldGUgcGF1c2UAb25fdXJsX2NvbXBsZXRlIHBhdXNlAG9uX2NodW5rX2NvbXBsZXRlIHBhdXNlAG9uX2hlYWRlcl92YWx1ZV9jb21wbGV0ZSBwYXVzZQBvbl9tZXNzYWdlX2NvbXBsZXRlIHBhdXNlAG9uX21ldGhvZF9jb21wbGV0ZSBwYXVzZQBvbl9oZWFkZXJfZmllbGRfY29tcGxldGUgcGF1c2UAb25fY2h1bmtfZXh0ZW5zaW9uX25hbWUgcGF1c2UAVW5leHBlY3RlZCBzcGFjZSBhZnRlciBzdGFydCBsaW5lAFNwYW4gY2FsbGJhY2sgZXJyb3IgaW4gb25fY2h1bmtfZXh0ZW5zaW9uX25hbWUASW52YWxpZCBjaGFyYWN0ZXIgaW4gY2h1bmsgZXh0ZW5zaW9ucyBuYW1lAFBhdXNlIG9uIENPTk5FQ1QvVXBncmFkZQBQYXVzZSBvbiBQUkkvVXBncmFkZQBFeHBlY3RlZCBIVFRQLzIgQ29ubmVjdGlvbiBQcmVmYWNlAFNwYW4gY2FsbGJhY2sgZXJyb3IgaW4gb25fbWV0aG9kAEV4cGVjdGVkIHNwYWNlIGFmdGVyIG1ldGhvZABTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX2hlYWRlcl9maWVsZABQYXVzZWQASW52YWxpZCB3b3JkIGVuY291bnRlcmVkAEludmFsaWQgbWV0aG9kIGVuY291bnRlcmVkAFVuZXhwZWN0ZWQgY2hhciBpbiB1cmwgc2NoZW1hAFJlcXVlc3QgaGFzIGludmFsaWQgYFRyYW5zZmVyLUVuY29kaW5nYABTV0lUQ0hfUFJPWFkAVVNFX1BST1hZAE1LQUNUSVZJVFkAVU5QUk9DRVNTQUJMRV9FTlRJVFkAQ09QWQBNT1ZFRF9QRVJNQU5FTlRMWQBUT09fRUFSTFkATk9USUZZAEZBSUxFRF9ERVBFTkRFTkNZAEJBRF9HQVRFV0FZAFBMQVkAUFVUAENIRUNLT1VUAEdBVEVXQVlfVElNRU9VVABSRVFVRVNUX1RJTUVPVVQATkVUV09SS19DT05ORUNUX1RJTUVPVVQAQ09OTkVDVElPTl9USU1FT1VUAExPR0lOX1RJTUVPVVQATkVUV09SS19SRUFEX1RJTUVPVVQAUE9TVABNSVNESVJFQ1RFRF9SRVFVRVNUAENMSUVOVF9DTE9TRURfUkVRVUVTVABDTElFTlRfQ0xPU0VEX0xPQURfQkFMQU5DRURfUkVRVUVTVABCQURfUkVRVUVTVABIVFRQX1JFUVVFU1RfU0VOVF9UT19IVFRQU19QT1JUAFJFUE9SVABJTV9BX1RFQVBPVABSRVNFVF9DT05URU5UAE5PX0NPTlRFTlQAUEFSVElBTF9DT05URU5UAEhQRV9JTlZBTElEX0NPTlNUQU5UAEhQRV9DQl9SRVNFVABHRVQASFBFX1NUUklDVABDT05GTElDVABURU1QT1JBUllfUkVESVJFQ1QAUEVSTUFORU5UX1JFRElSRUNUAENPTk5FQ1QATVVMVElfU1RBVFVTAEhQRV9JTlZBTElEX1NUQVRVUwBUT09fTUFOWV9SRVFVRVNUUwBFQVJMWV9ISU5UUwBVTkFWQUlMQUJMRV9GT1JfTEVHQUxfUkVBU09OUwBPUFRJT05TAFNXSVRDSElOR19QUk9UT0NPTFMAVkFSSUFOVF9BTFNPX05FR09USUFURVMATVVMVElQTEVfQ0hPSUNFUwBJTlRFUk5BTF9TRVJWRVJfRVJST1IAV0VCX1NFUlZFUl9VTktOT1dOX0VSUk9SAFJBSUxHVU5fRVJST1IASURFTlRJVFlfUFJPVklERVJfQVVUSEVOVElDQVRJT05fRVJST1IAU1NMX0NFUlRJRklDQVRFX0VSUk9SAElOVkFMSURfWF9GT1JXQVJERURfRk9SAFNFVF9QQVJBTUVURVIAR0VUX1BBUkFNRVRFUgBIUEVfVVNFUgBTRUVfT1RIRVIASFBFX0NCX0NIVU5LX0hFQURFUgBNS0NBTEVOREFSAFNFVFVQAFdFQl9TRVJWRVJfSVNfRE9XTgBURUFSRE9XTgBIUEVfQ0xPU0VEX0NPTk5FQ1RJT04ASEVVUklTVElDX0VYUElSQVRJT04ARElTQ09OTkVDVEVEX09QRVJBVElPTgBOT05fQVVUSE9SSVRBVElWRV9JTkZPUk1BVElPTgBIUEVfSU5WQUxJRF9WRVJTSU9OAEhQRV9DQl9NRVNTQUdFX0JFR0lOAFNJVEVfSVNfRlJPWkVOAEhQRV9JTlZBTElEX0hFQURFUl9UT0tFTgBJTlZBTElEX1RPS0VOAEZPUkJJRERFTgBFTkhBTkNFX1lPVVJfQ0FMTQBIUEVfSU5WQUxJRF9VUkwAQkxPQ0tFRF9CWV9QQVJFTlRBTF9DT05UUk9MAE1LQ09MAEFDTABIUEVfSU5URVJOQUwAUkVRVUVTVF9IRUFERVJfRklFTERTX1RPT19MQVJHRV9VTk9GRklDSUFMAEhQRV9PSwBVTkxJTksAVU5MT0NLAFBSSQBSRVRSWV9XSVRIAEhQRV9JTlZBTElEX0NPTlRFTlRfTEVOR1RIAEhQRV9VTkVYUEVDVEVEX0NPTlRFTlRfTEVOR1RIAEZMVVNIAFBST1BQQVRDSABNLVNFQVJDSABVUklfVE9PX0xPTkcAUFJPQ0VTU0lORwBNSVNDRUxMQU5FT1VTX1BFUlNJU1RFTlRfV0FSTklORwBNSVNDRUxMQU5FT1VTX1dBUk5JTkcASFBFX0lOVkFMSURfVFJBTlNGRVJfRU5DT0RJTkcARXhwZWN0ZWQgQ1JMRgBIUEVfSU5WQUxJRF9DSFVOS19TSVpFAE1PVkUAQ09OVElOVUUASFBFX0NCX1NUQVRVU19DT01QTEVURQBIUEVfQ0JfSEVBREVSU19DT01QTEVURQBIUEVfQ0JfVkVSU0lPTl9DT01QTEVURQBIUEVfQ0JfVVJMX0NPTVBMRVRFAEhQRV9DQl9DSFVOS19DT01QTEVURQBIUEVfQ0JfSEVBREVSX1ZBTFVFX0NPTVBMRVRFAEhQRV9DQl9DSFVOS19FWFRFTlNJT05fVkFMVUVfQ09NUExFVEUASFBFX0NCX0NIVU5LX0VYVEVOU0lPTl9OQU1FX0NPTVBMRVRFAEhQRV9DQl9NRVNTQUdFX0NPTVBMRVRFAEhQRV9DQl9NRVRIT0RfQ09NUExFVEUASFBFX0NCX0hFQURFUl9GSUVMRF9DT01QTEVURQBERUxFVEUASFBFX0lOVkFMSURfRU9GX1NUQVRFAElOVkFMSURfU1NMX0NFUlRJRklDQVRFAFBBVVNFAE5PX1JFU1BPTlNFAFVOU1VQUE9SVEVEX01FRElBX1RZUEUAR09ORQBOT1RfQUNDRVBUQUJMRQBTRVJWSUNFX1VOQVZBSUxBQkxFAFJBTkdFX05PVF9TQVRJU0ZJQUJMRQBPUklHSU5fSVNfVU5SRUFDSEFCTEUAUkVTUE9OU0VfSVNfU1RBTEUAUFVSR0UATUVSR0UAUkVRVUVTVF9IRUFERVJfRklFTERTX1RPT19MQVJHRQBSRVFVRVNUX0hFQURFUl9UT09fTEFSR0UAUEFZTE9BRF9UT09fTEFSR0UASU5TVUZGSUNJRU5UX1NUT1JBR0UASFBFX1BBVVNFRF9VUEdSQURFAEhQRV9QQVVTRURfSDJfVVBHUkFERQBTT1VSQ0UAQU5OT1VOQ0UAVFJBQ0UASFBFX1VORVhQRUNURURfU1BBQ0UAREVTQ1JJQkUAVU5TVUJTQ1JJQkUAUkVDT1JEAEhQRV9JTlZBTElEX01FVEhPRABOT1RfRk9VTkQAUFJPUEZJTkQAVU5CSU5EAFJFQklORABVTkFVVEhPUklaRUQATUVUSE9EX05PVF9BTExPV0VEAEhUVFBfVkVSU0lPTl9OT1RfU1VQUE9SVEVEAEFMUkVBRFlfUkVQT1JURUQAQUNDRVBURUQATk9UX0lNUExFTUVOVEVEAExPT1BfREVURUNURUQASFBFX0NSX0VYUEVDVEVEAEhQRV9MRl9FWFBFQ1RFRABDUkVBVEVEAElNX1VTRUQASFBFX1BBVVNFRABUSU1FT1VUX09DQ1VSRUQAUEFZTUVOVF9SRVFVSVJFRABQUkVDT05ESVRJT05fUkVRVUlSRUQAUFJPWFlfQVVUSEVOVElDQVRJT05fUkVRVUlSRUQATkVUV09SS19BVVRIRU5USUNBVElPTl9SRVFVSVJFRABMRU5HVEhfUkVRVUlSRUQAU1NMX0NFUlRJRklDQVRFX1JFUVVJUkVEAFVQR1JBREVfUkVRVUlSRUQAUEFHRV9FWFBJUkVEAFBSRUNPTkRJVElPTl9GQUlMRUQARVhQRUNUQVRJT05fRkFJTEVEAFJFVkFMSURBVElPTl9GQUlMRUQAU1NMX0hBTkRTSEFLRV9GQUlMRUQATE9DS0VEAFRSQU5TRk9STUFUSU9OX0FQUExJRUQATk9UX01PRElGSUVEAE5PVF9FWFRFTkRFRABCQU5EV0lEVEhfTElNSVRfRVhDRUVERUQAU0lURV9JU19PVkVSTE9BREVEAEhFQUQARXhwZWN0ZWQgSFRUUC8AAF4TAAAmEwAAMBAAAPAXAACdEwAAFRIAADkXAADwEgAAChAAAHUSAACtEgAAghMAAE8UAAB/EAAAoBUAACMUAACJEgAAixQAAE0VAADUEQAAzxQAABAYAADJFgAA3BYAAMERAADgFwAAuxQAAHQUAAB8FQAA5RQAAAgXAAAfEAAAZRUAAKMUAAAoFQAAAhUAAJkVAAAsEAAAixkAAE8PAADUDgAAahAAAM4QAAACFwAAiQ4AAG4TAAAcEwAAZhQAAFYXAADBEwAAzRMAAGwTAABoFwAAZhcAAF8XAAAiEwAAzg8AAGkOAADYDgAAYxYAAMsTAACqDgAAKBcAACYXAADFEwAAXRYAAOgRAABnEwAAZRMAAPIWAABzEwAAHRcAAPkWAADzEQAAzw4AAM4VAAAMEgAAsxEAAKURAABhEAAAMhcAALsTAEH5NQsBAQBBkDYL4AEBAQIBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEAAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQBB/TcLAQEAQZE4C14CAwICAgICAAACAgACAgACAgICAgICAgICAAQAAAAAAAICAgICAgICAgICAgICAgICAgICAgICAgICAAAAAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAAgACAEH9OQsBAQBBkToLXgIAAgICAgIAAAICAAICAAICAgICAgICAgIAAwAEAAAAAgICAgICAgICAgICAgICAgICAgICAgICAgIAAAACAgICAgICAgICAgICAgICAgICAgICAgICAgICAgACAAIAQfA7Cw1sb3NlZWVwLWFsaXZlAEGJPAsBAQBBoDwL4AEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQBBiT4LAQEAQaA+C+cBAQEBAQEBAQEBAQEBAgEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEAAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQFjaHVua2VkAEGwwAALXwEBAAEBAQEBAAABAQABAQABAQEBAQEBAQEBAAAAAAAAAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAAAAAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEAAQABAEGQwgALIWVjdGlvbmVudC1sZW5ndGhvbnJveHktY29ubmVjdGlvbgBBwMIACy1yYW5zZmVyLWVuY29kaW5ncGdyYWRlDQoNCg0KU00NCg0KVFRQL0NFL1RTUC8AQfnCAAsFAQIAAQMAQZDDAAvgAQQBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAEH5xAALBQECAAEDAEGQxQAL4AEEAQEFAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQBB+cYACwQBAAABAEGRxwAL3wEBAQABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEAAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAEH6yAALBAEAAAIAQZDJAAtfAwQAAAQEBAQEBAQEBAQEBQQEBAQEBAQEBAQEBAAEAAYHBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEAAQABAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAAAAAQAQfrKAAsEAQAAAQBBkMsACwEBAEGqywALQQIAAAAAAAADAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwAAAAAAAAMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAEH6zAALBAEAAAEAQZDNAAsBAQBBms0ACwYCAAAAAAIAQbHNAAs6AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMAAAAAAAADAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwBB8M4AC5YBTk9VTkNFRUNLT1VUTkVDVEVURUNSSUJFTFVTSEVURUFEU0VBUkNIUkdFQ1RJVklUWUxFTkRBUlZFT1RJRllQVElPTlNDSFNFQVlTVEFUQ0hHRU9SRElSRUNUT1JUUkNIUEFSQU1FVEVSVVJDRUJTQ1JJQkVBUkRPV05BQ0VJTkROS0NLVUJTQ1JJQkVIVFRQL0FEVFAv","base64")}function be(){if(FA)return bA;FA=1;const A=/** @type {const} */["GET","HEAD","POST"],e=new Set(A),t=/** @type {const} */[301,302,303,307,308],r=new Set(t),n=/** @type {const} */["1","7","9","11","13","15","17","19","20","21","22","23","25","37","42","43","53","69","77","79","87","95","101","102","103","104","109","110","111","113","115","117","119","123","135","137","139","143","161","179","389","427","465","512","513","514","515","526","530","531","532","540","548","554","556","563","587","601","636","989","990","993","995","1719","1720","1723","2049","3659","4045","4190","5060","5061","6000","6566","6665","6666","6667","6668","6669","6679","6697","10080"],s=new Set(n),o=/** @type {const} */["","no-referrer","no-referrer-when-downgrade","same-origin","origin","strict-origin","origin-when-cross-origin","strict-origin-when-cross-origin","unsafe-url"],i=new Set(o),a=/** @type {const} */["GET","HEAD","OPTIONS","TRACE"],g=new Set(a),c=/** @type {const} */["CONNECT","TRACE","TRACK"],Q=new Set(c),E=/** @type {const} */["audio","audioworklet","font","image","manifest","paintworklet","script","style","track","video","xslt",""],B=new Set(E);return bA={subresource:E,forbiddenMethods:c,requestBodyHeader:/** @type {const} */["content-encoding","content-language","content-location","content-type",
// See https://github.com/nodejs/undici/issues/2021
// 'Content-Length' is a forbidden header name, which is typically
// removed in the Headers implementation. However, undici doesn't
// filter out headers, so we add it here.
"content-length"],referrerPolicy:o,requestRedirect:/** @type {const} */["follow","manual","error"],requestMode:/** @type {const} */["navigate","same-origin","no-cors","cors"],requestCredentials:/** @type {const} */["omit","same-origin","include"],requestCache:/** @type {const} */["default","no-store","reload","no-cache","force-cache","only-if-cached"],redirectStatus:t,corsSafeListedMethods:A,nullBodyStatus:/** @type {const} */[101,204,205,304],safeMethods:a,badPorts:n,requestDuplex:/** @type {const} */["half"],subresourceSet:B,badPortsSet:s,redirectStatusSet:r,corsSafeListedMethodsSet:e,safeMethodsSet:g,forbiddenMethodsSet:Q,referrerPolicySet:i}}function Fe(){if(NA)return SA;NA=1;
// In case of breaking changes, increase the version
// number to avoid conflicts.
const A=Symbol.for("undici.globalOrigin.1");return SA={getGlobalOrigin:function(){return globalThis[A]},setGlobalOrigin:function(e){if(void 0===e)return void Object.defineProperty(globalThis,A,{value:void 0,writable:!0,enumerable:!1,configurable:!1});const t=new URL(e);if("http:"!==t.protocol&&"https:"!==t.protocol)throw new TypeError(`Only http & https urls are allowed, received ${t.protocol}`);Object.defineProperty(globalThis,A,{value:t,writable:!0,enumerable:!1,configurable:!1})}}}function Se(){if(UA)return MA;UA=1;const A=E,e=new TextEncoder,t=/^[!#$%&'*+\-.^_|~A-Za-z0-9]+$/,r=/[\u000A\u000D\u0009\u0020]/,n=/[\u0009\u000A\u000C\u000D\u0020]/g,s=/^[\u0009\u0020-\u007E\u0080-\u00FF]+$/;
// https://url.spec.whatwg.org/#concept-url-serializer
/**
	 * @param {URL} url
	 * @param {boolean} excludeFragment
	 */
function o(A,e=!1){if(!e)return A.href;const t=A.href,r=A.hash.length,n=0===r?t:t.substring(0,t.length-r);return!r&&t.endsWith("#")?n.slice(0,-1):n}
// https://infra.spec.whatwg.org/#collect-a-sequence-of-code-points
/**
	 * @param {(char: string) => boolean} condition
	 * @param {string} input
	 * @param {{ position: number }} position
	 */function i(A,e,t){
// 1. Let result be the empty string.
let r="";
// 2. While position doesn’t point past the end of input and the
// code point at position within input meets the condition condition:
for(;t.position<e.length&&A(e[t.position]);)
// 1. Append that code point to the end of result.
r+=e[t.position],
// 2. Advance position by 1.
t.position++;
// 3. Return result.
return r}
/**
	 * A faster collectASequenceOfCodePoints that only works when comparing a single character.
	 * @param {string} char
	 * @param {string} input
	 * @param {{ position: number }} position
	 */function a(A,e,t){const r=e.indexOf(A,t.position),n=t.position;return-1===r?(t.position=e.length,e.slice(n)):(t.position=r,e.slice(n,t.position))}
// https://url.spec.whatwg.org/#string-percent-decode
/** @param {string} input */function g(A){
// 2. Return the percent-decoding of bytes.
// https://url.spec.whatwg.org/#percent-decode
/** @param {Uint8Array} input */
return function(A){const e=A.length,t=new Uint8Array(e);
// 1. Let output be an empty byte sequence.
/** @type {Uint8Array} */let r=0;
// 2. For each byte byte in input:
for(let n=0;n<e;++n){const e=A[n];
// 1. If byte is not 0x25 (%), then append byte to output.
37!==e?t[r++]=e:37!==e||c(A[n+1])&&c(A[n+2])?(
// 1. Let bytePoint be the two bytes after byte in input,
// decoded, and then interpreted as hexadecimal number.
// 2. Append a byte whose value is bytePoint to output.
t[r++]=Q(A[n+1])<<4|Q(A[n+2]),
// 3. Skip the next two bytes in input.
n+=2):t[r++]=37}
// 3. Return output.
return e===r?t:t.subarray(0,r)}
// https://mimesniff.spec.whatwg.org/#parse-a-mime-type
/** @param {string} input */(e.encode(A))}
/**
	 * @param {number} byte
	 */function c(A){
// 0-9 A-F a-f
return A>=48&&A<=57||A>=65&&A<=70||A>=97&&A<=102}
/**
	 * @param {number} byte
	 */function Q(A){
// 0-9
return A>=48&&A<=57?A-48:(223&A)-55}function B(A){
// 2. Let position be a position variable for input,
// initially pointing at the start of input.
const e={position:0},n=a("/",
// 1. Remove any leading and trailing HTTP whitespace
// from input.
A=C(A,!0,!0),e);
// 3. Let type be the result of collecting a sequence
// of code points that are not U+002F (/) from
// input, given position.
// 4. If type is the empty string or does not solely
// contain HTTP token code points, then return failure.
// https://mimesniff.spec.whatwg.org/#http-token-code-point
if(0===n.length||!t.test(n))return"failure";
// 5. If position is past the end of input, then return
// failure
if(e.position>A.length)return"failure";
// 6. Advance position by 1. (This skips past U+002F (/).)
e.position++;
// 7. Let subtype be the result of collecting a sequence of
// code points that are not U+003B (;) from input, given
// position.
let o=a(";",A,e);
// 8. Remove any trailing HTTP whitespace from subtype.
// 9. If subtype is the empty string or does not solely
// contain HTTP token code points, then return failure.
if(o=C(o,!1,!0),0===o.length||!t.test(o))return"failure";const g=n.toLowerCase(),c=o.toLowerCase(),Q={type:g,subtype:c,
/** @type {Map<string, string>} */
parameters:new Map,
// https://mimesniff.spec.whatwg.org/#mime-type-essence
essence:`${g}/${c}`};
// 11. While position is not past the end of input:
for(;e.position<A.length;){
// 1. Advance position by 1. (This skips past U+003B (;).)
e.position++,
// 2. Collect a sequence of code points that are HTTP
// whitespace from input given position.
i(
// https://fetch.spec.whatwg.org/#http-whitespace
A=>r.test(A),A,e);
// 3. Let parameterName be the result of collecting a
// sequence of code points that are not U+003B (;)
// or U+003D (=) from input, given position.
let n=i(A=>";"!==A&&"="!==A,A,e);
// 4. Set parameterName to parameterName, in ASCII
// lowercase.
// 5. If position is not past the end of input, then:
if(n=n.toLowerCase(),e.position<A.length){
// 1. If the code point at position within input is
// U+003B (;), then continue.
if(";"===A[e.position])continue;
// 2. Advance position by 1. (This skips past U+003D (=).)
e.position++}
// 6. If position is past the end of input, then break.
if(e.position>A.length)break;
// 7. Let parameterValue be null.
let o=null;
// 8. If the code point at position within input is
// U+0022 ("), then:
if('"'===A[e.position])
// 1. Set parameterValue to the result of collecting
// an HTTP quoted string from input, given position
// and the extract-value flag.
o=h(A,e,!0),
// 2. Collect a sequence of code points that are not
// U+003B (;) from input, given position.
a(";",A,e);else
// 3. If parameterValue is the empty string, then continue.
if(
// 1. Set parameterValue to the result of collecting
// a sequence of code points that are not U+003B (;)
// from input, given position.
o=a(";",A,e),
// 2. Remove any trailing HTTP whitespace from parameterValue.
o=C(o,!1,!0),0===o.length)continue;
// 10. If all of the following are true
// - parameterName is not the empty string
// - parameterName solely contains HTTP token code points
// - parameterValue solely contains HTTP quoted-string token code points
// - mimeType’s parameters[parameterName] does not exist
// then set mimeType’s parameters[parameterName] to parameterValue.
0===n.length||!t.test(n)||0!==o.length&&!s.test(o)||Q.parameters.has(n)||Q.parameters.set(n,o)}
// 12. Return mimeType.
return Q}
// https://infra.spec.whatwg.org/#forgiving-base64-decode
/** @param {string} data */
// https://fetch.spec.whatwg.org/#collect-an-http-quoted-string
// tests: https://fetch.spec.whatwg.org/#example-http-quoted-string
/**
	 * @param {string} input
	 * @param {{ position: number }} position
	 * @param {boolean?} extractValue
	 */
function h(e,t,r){
// 1. Let positionStart be position.
const n=t.position;
// 2. Let value be the empty string.
let s="";
// 3. Assert: the code point at position within input
// is U+0022 (").
// 5. While true:
for(A('"'===e[t.position]),
// 4. Advance position by 1.
t.position++;
// 1. Append the result of collecting a sequence of code points
// that are not U+0022 (") or U+005C (\) from input, given
// position, to value.
s+=i(A=>'"'!==A&&"\\"!==A,e,t),!(t.position>=e.length);){
// 3. Let quoteOrBackslash be the code point at position within
// input.
const r=e[t.position];
// 4. Advance position by 1.
// 5. If quoteOrBackslash is U+005C (\), then:
if(t.position++,"\\"!==r){
// 1. Assert: quoteOrBackslash is U+0022 (").
A('"'===r);
// 2. Break.
break}
// 1. If position is past the end of input, then append
// U+005C (\) to value and break.
if(t.position>=e.length){s+="\\";break}
// 2. Append the code point at position within input to value.
s+=e[t.position],
// 3. Advance position by 1.
t.position++}
// 6. If the extract-value flag is set, then return value.
return r?s:e.slice(n,t.position);
// 7. Return the code points from positionStart to position,
// inclusive, within input.
}
/**
	 * @see https://mimesniff.spec.whatwg.org/#serialize-a-mime-type
	 */
/**
	 * @see https://fetch.spec.whatwg.org/#http-whitespace
	 * @param {number} char
	 */
function I(A){
// "\r\n\t "
return 13===A||10===A||9===A||32===A}
/**
	 * @see https://fetch.spec.whatwg.org/#http-whitespace
	 * @param {string} str
	 * @param {boolean} [leading=true]
	 * @param {boolean} [trailing=true]
	 */function C(A,e=!0,t=!0){return u(A,e,t,I)}
/**
	 * @see https://infra.spec.whatwg.org/#ascii-whitespace
	 * @param {number} char
	 */function l(A){
// "\r\n\t\f "
return 13===A||10===A||9===A||12===A||32===A}
/**
	 * @see https://infra.spec.whatwg.org/#strip-leading-and-trailing-ascii-whitespace
	 * @param {string} str
	 * @param {boolean} [leading=true]
	 * @param {boolean} [trailing=true]
	 */
/**
	 * @param {string} str
	 * @param {boolean} leading
	 * @param {boolean} trailing
	 * @param {(charCode: number) => boolean} predicate
	 * @returns
	 */
function u(A,e,t,r){let n=0,s=A.length-1;if(e)for(;n<A.length&&r(A.charCodeAt(n));)n++;if(t)for(;s>0&&r(A.charCodeAt(s));)s--;return 0===n&&s===A.length-1?A:A.slice(n,s+1)}
/**
	 * @see https://infra.spec.whatwg.org/#isomorphic-decode
	 * @param {Uint8Array} input
	 * @returns {string}
	 */function d(A){
// 1. To isomorphic decode a byte sequence input, return a string whose code point
//    length is equal to input’s length and whose code points have the same values
//    as the values of input’s bytes, in the same order.
const e=A.length;if(65535>e)return String.fromCharCode.apply(null,A);let t="",r=0,n=65535;for(;r<e;)r+n>e&&(n=e-r),t+=String.fromCharCode.apply(null,A.subarray(r,r+=n));return t}
/**
	 * @see https://mimesniff.spec.whatwg.org/#minimize-a-supported-mime-type
	 * @param {Exclude<ReturnType<typeof parseMIMEType>, 'failure'>} mimeType
	 */return MA={dataURLProcessor:// eslint-disable-line
// https://fetch.spec.whatwg.org/#data-url-processor
/** @param {URL} dataURL */
function(e){
// 1. Assert: dataURL’s scheme is "data".
A("data:"===e.protocol);
// 2. Let input be the result of running the URL
// serializer on dataURL with exclude fragment
// set to true.
let t=o(e,!0);
// 3. Remove the leading "data:" string from input.
t=t.slice(5);
// 4. Let position point at the start of input.
const r={position:0};
// 5. Let mimeType be the result of collecting a
// sequence of code points that are not equal
// to U+002C (,), given position.
let s=a(",",t,r);
// 6. Strip leading and trailing ASCII whitespace
// from mimeType.
// Undici implementation note: we need to store the
// length because if the mimetype has spaces removed,
// the wrong amount will be sliced from the input in
// step #9
const i=s.length;
// 7. If position is past the end of input, then
// return failure
if(s=function(A,e=!0,t=!0){return u(A,e,t,l)}(s,!0,!0),r.position>=t.length)return"failure";
// 8. Advance position by 1.
r.position++;
// 10. Let body be the percent-decoding of encodedBody.
let c=g(t.slice(i+1));
// 11. If mimeType ends with U+003B (;), followed by
// zero or more U+0020 SPACE, followed by an ASCII
// case-insensitive match for "base64", then:
if(/;(\u0020){0,}base64$/i.test(s)){
// 3. If body is failure, then return failure.
if(
// 2. Set body to the forgiving-base64 decode of
// stringBody.
c=function(A){// eslint-disable-line
let e=(
// 1. Remove all ASCII whitespace from data.
A=A.replace(n,"")).length;
// 2. If data’s code point length divides by 4 leaving
// no remainder, then:
e%4==0&&61===A.charCodeAt(e-1)&&(--e,61===A.charCodeAt(e-1)&&--e);
// 3. If data’s code point length divides by 4 leaving
// a remainder of 1, then return failure.
if(e%4==1)return"failure";
// 4. If data contains a code point that is not one of
//  U+002B (+)
//  U+002F (/)
//  ASCII alphanumeric
// then return failure.
if(/[^+/0-9A-Za-z]/.test(A.length===e?A:A.substring(0,e)))return"failure";const t=Buffer.from(A,"base64");return new Uint8Array(t.buffer,t.byteOffset,t.byteLength)}(d(c)),"failure"===c)return"failure";
// 4. Remove the last 6 code points from mimeType.
s=s.slice(0,-6),
// 5. Remove trailing U+0020 SPACE code points from mimeType,
// if any.
s=s.replace(/(\u0020)+$/,""),
// 6. Remove the last U+003B (;) code point from mimeType.
s=s.slice(0,-1)}
// 12. If mimeType starts with U+003B (;), then prepend
// "text/plain" to mimeType.
s.startsWith(";")&&(s="text/plain"+s);
// 13. Let mimeTypeRecord be the result of parsing
// mimeType.
let Q=B(s);
// 14. If mimeTypeRecord is failure, then set
// mimeTypeRecord to text/plain;charset=US-ASCII.
// 15. Return a new data: URL struct whose MIME
// type is mimeTypeRecord and body is body.
// https://fetch.spec.whatwg.org/#data-url-struct
return"failure"===Q&&(Q=B("text/plain;charset=US-ASCII")),{mimeType:Q,body:c}},URLSerializer:o,collectASequenceOfCodePoints:i,collectASequenceOfCodePointsFast:a,stringPercentDecode:g,parseMIMEType:B,collectAnHTTPQuotedString:h,serializeAMimeType:function(e){A("failure"!==e);const{parameters:r,essence:n}=e;
// 1. Let serialization be the concatenation of mimeType’s
//    type, U+002F (/), and mimeType’s subtype.
let s=n;
// 2. For each name → value of mimeType’s parameters:
for(let[A,e]of r.entries())
// 1. Append U+003B (;) to serialization.
s+=";",
// 2. Append name to serialization.
s+=A,
// 3. Append U+003D (=) to serialization.
s+="=",
// 4. If value does not solely contain HTTP token code
//    points or value is the empty string, then:
t.test(e)||(
// 1. Precede each occurrence of U+0022 (") or
//    U+005C (\) in value with U+005C (\).
e=e.replace(/(\\|")/g,"\\$1"),
// 2. Prepend U+0022 (") to value.
e='"'+e,
// 3. Append U+0022 (") to value.
e+='"'),
// 5. Append value to serialization.
s+=e;
// 3. Return serialization.
return s},removeChars:u,removeHTTPWhitespace:C,minimizeSupportedMimeType:function(A){switch(A.essence){case"application/ecmascript":case"application/javascript":case"application/x-ecmascript":case"application/x-javascript":case"text/ecmascript":case"text/javascript":case"text/javascript1.0":case"text/javascript1.1":case"text/javascript1.2":case"text/javascript1.3":case"text/javascript1.4":case"text/javascript1.5":case"text/jscript":case"text/livescript":case"text/x-ecmascript":case"text/x-javascript":
// 1. If mimeType is a JavaScript MIME type, then return "text/javascript".
return"text/javascript";case"application/json":case"text/json":
// 2. If mimeType is a JSON MIME type, then return "application/json".
return"application/json";case"image/svg+xml":
// 3. If mimeType’s essence is "image/svg+xml", then return "image/svg+xml".
return"image/svg+xml";case"text/xml":case"application/xml":
// 4. If mimeType is an XML MIME type, then return "application/xml".
return"application/xml"}
// 2. If mimeType is a JSON MIME type, then return "application/json".
return A.subtype.endsWith("+json")?"application/json":
// 4. If mimeType is an XML MIME type, then return "application/xml".
A.subtype.endsWith("+xml")?"application/xml":""},HTTP_TOKEN_CODEPOINTS:t,isomorphicDecode:d},MA}function Ne(){if(TA)return LA;TA=1;const{types:A,inspect:e}=l,{markAsUncloneable:t}=m,{toUSVString:r}=CA(),n={converters:{},util:{},errors:{}};return n.errors.exception=function(A){return new TypeError(`${A.header}: ${A.message}`)},n.errors.conversionFailed=function(A){const e=1===A.types.length?"":" one of",t=`${A.argument} could not be converted to${e}: ${A.types.join(", ")}.`;return n.errors.exception({header:A.prefix,message:t})},n.errors.invalidArgument=function(A){return n.errors.exception({header:A.prefix,message:`"${A.value}" is an invalid ${A.type}.`})},
// https://webidl.spec.whatwg.org/#implements
n.brandCheck=function(A,e,t){if(!1!==t?.strict){if(!(A instanceof e)){const A=new TypeError("Illegal invocation");// node compat.
throw A.code="ERR_INVALID_THIS",A}}else if(A?.[Symbol.toStringTag]!==e.prototype[Symbol.toStringTag]){const A=new TypeError("Illegal invocation");// node compat.
throw A.code="ERR_INVALID_THIS",A}},n.argumentLengthCheck=function({length:A},e,t){if(A<e)throw n.errors.exception({message:`${e} argument${1!==e?"s":""} required, but${A?" only":""} ${A} found.`,header:t})},n.illegalConstructor=function(){throw n.errors.exception({header:"TypeError",message:"Illegal constructor"})},
// https://tc39.es/ecma262/#sec-ecmascript-data-types-and-values
n.util.Type=function(A){switch(typeof A){case"undefined":return"Undefined";case"boolean":return"Boolean";case"string":return"String";case"symbol":return"Symbol";case"number":return"Number";case"bigint":return"BigInt";case"function":case"object":return null===A?"Null":"Object"}},n.util.markAsUncloneable=t||(()=>{}),
// https://webidl.spec.whatwg.org/#abstract-opdef-converttoint
n.util.ConvertToInt=function(A,e,t,r){let s,o;
// 1. If bitLength is 64, then:
64===e?(
// 1. Let upperBound be 2^53 − 1.
s=Math.pow(2,53)-1,
// 2. If signedness is "unsigned", then let lowerBound be 0.
o="unsigned"===t?0:Math.pow(-2,53)+1):"unsigned"===t?(
// 2. Otherwise, if signedness is "unsigned", then:
// 1. Let lowerBound be 0.
o=0,
// 2. Let upperBound be 2^bitLength − 1.
s=Math.pow(2,e)-1):(
// 3. Otherwise:
// 1. Let lowerBound be -2^bitLength − 1.
o=Math.pow(-2,e)-1,
// 2. Let upperBound be 2^bitLength − 1 − 1.
s=Math.pow(2,e-1)-1);
// 4. Let x be ? ToNumber(V).
let i=Number(A);
// 5. If x is −0, then set x to +0.
// 6. If the conversion is to an IDL type associated
//    with the [EnforceRange] extended attribute, then:
if(0===i&&(i=0),!0===r?.enforceRange){
// 1. If x is NaN, +∞, or −∞, then throw a TypeError.
if(Number.isNaN(i)||i===Number.POSITIVE_INFINITY||i===Number.NEGATIVE_INFINITY)throw n.errors.exception({header:"Integer conversion",message:`Could not convert ${n.util.Stringify(A)} to an integer.`});
// 2. Set x to IntegerPart(x).
// 3. If x < lowerBound or x > upperBound, then
//    throw a TypeError.
if(i=n.util.IntegerPart(i),i<o||i>s)throw n.errors.exception({header:"Integer conversion",message:`Value must be between ${o}-${s}, got ${i}.`});
// 4. Return x.
return i}
// 7. If x is not NaN and the conversion is to an IDL
//    type associated with the [Clamp] extended
//    attribute, then:
return Number.isNaN(i)||!0!==r?.clamp?
// 8. If x is NaN, +0, +∞, or −∞, then return +0.
Number.isNaN(i)||0===i&&Object.is(0,i)||i===Number.POSITIVE_INFINITY||i===Number.NEGATIVE_INFINITY?0:(
// 9. Set x to IntegerPart(x).
i=n.util.IntegerPart(i),
// 10. Set x to x modulo 2^bitLength.
i%=Math.pow(2,e),
// 11. If signedness is "signed" and x ≥ 2^bitLength − 1,
//    then return x − 2^bitLength.
"signed"===t&&i>=Math.pow(2,e)-1?i-Math.pow(2,e):i):(
// 1. Set x to min(max(x, lowerBound), upperBound).
i=Math.min(Math.max(i,o),s),
// 2. Round x to the nearest integer, choosing the
//    even integer if it lies halfway between two,
//    and choosing +0 rather than −0.
i=Math.floor(i)%2==0?Math.floor(i):Math.ceil(i),i)},
// https://webidl.spec.whatwg.org/#abstract-opdef-integerpart
n.util.IntegerPart=function(A){
// 1. Let r be floor(abs(n)).
const e=Math.floor(Math.abs(A));
// 2. If n < 0, then return -1 × r.
return A<0?-1*e:e;
// 3. Otherwise, return r.
},n.util.Stringify=function(A){switch(n.util.Type(A)){case"Symbol":return`Symbol(${A.description})`;case"Object":return e(A);case"String":return`"${A}"`;default:return`${A}`}},
// https://webidl.spec.whatwg.org/#es-sequence
n.sequenceConverter=function(A){return(e,t,r,s)=>{
// 1. If Type(V) is not Object, throw a TypeError.
if("Object"!==n.util.Type(e))throw n.errors.exception({header:t,message:`${r} (${n.util.Stringify(e)}) is not iterable.`});
// 2. Let method be ? GetMethod(V, @@iterator).
/** @type {Generator} */const o="function"==typeof s?s():e?.[Symbol.iterator]?.(),i=[];let a=0;
// 3. If method is undefined, throw a TypeError.
if(void 0===o||"function"!=typeof o.next)throw n.errors.exception({header:t,message:`${r} is not iterable.`});
// https://webidl.spec.whatwg.org/#create-sequence-from-iterable
for(;;){const{done:e,value:n}=o.next();if(e)break;i.push(A(n,t,`${r}[${a++}]`))}return i}},
// https://webidl.spec.whatwg.org/#es-to-record
n.recordConverter=function(e,t){return(r,s,o)=>{
// 1. If Type(O) is not Object, throw a TypeError.
if("Object"!==n.util.Type(r))throw n.errors.exception({header:s,message:`${o} ("${n.util.Type(r)}") is not an Object.`});
// 2. Let result be a new empty instance of record<K, V>.
const i={};if(!A.isProxy(r)){
// 1. Let desc be ? O.[[GetOwnProperty]](key).
const A=[...Object.getOwnPropertyNames(r),...Object.getOwnPropertySymbols(r)];for(const n of A){
// 1. Let typedKey be key converted to an IDL value of type K.
const A=e(n,s,o),a=t(r[n],s,o);
// 2. Let value be ? Get(O, key).
// 3. Let typedValue be value converted to an IDL value of type V.
// 4. Set result[typedKey] to typedValue.
i[A]=a}
// 5. Return result.
return i}
// 3. Let keys be ? O.[[OwnPropertyKeys]]().
const a=Reflect.ownKeys(r);
// 4. For each key of keys.
for(const A of a){
// 1. Let desc be ? O.[[GetOwnProperty]](key).
const n=Reflect.getOwnPropertyDescriptor(r,A);
// 2. If desc is not undefined and desc.[[Enumerable]] is true:
if(n?.enumerable){
// 1. Let typedKey be key converted to an IDL value of type K.
const n=e(A,s,o),a=t(r[A],s,o);
// 2. Let value be ? Get(O, key).
// 3. Let typedValue be value converted to an IDL value of type V.
// 4. Set result[typedKey] to typedValue.
i[n]=a}}
// 5. Return result.
return i}},n.interfaceConverter=function(A){return(e,t,r,s)=>{if(!1!==s?.strict&&!(e instanceof A))throw n.errors.exception({header:t,message:`Expected ${r} ("${n.util.Stringify(e)}") to be an instance of ${A.name}.`});return e}},n.dictionaryConverter=function(A){return(e,t,r)=>{const s=n.util.Type(e),o={};if("Null"===s||"Undefined"===s)return o;if("Object"!==s)throw n.errors.exception({header:t,message:`Expected ${e} to be one of: Null, Undefined, Object.`});for(const s of A){const{key:A,defaultValue:i,required:a,converter:g}=s;if(!0===a&&!Object.hasOwn(e,A))throw n.errors.exception({header:t,message:`Missing required key "${A}".`});let c=e[A];const Q=Object.hasOwn(s,"defaultValue");
// Only use defaultValue if value is undefined and
// a defaultValue options was provided.
// A key can be optional and have no default value.
// When this happens, do not perform a conversion,
// and do not assign the key a value.
if(Q&&null!==c&&(c??=i()),a||Q||void 0!==c){if(c=g(c,t,`${r}.${A}`),s.allowedValues&&!s.allowedValues.includes(c))throw n.errors.exception({header:t,message:`${c} is not an accepted type. Expected one of ${s.allowedValues.join(", ")}.`});o[A]=c}}return o}},n.nullableConverter=function(A){return(e,t,r)=>null===e?e:A(e,t,r)},
// https://webidl.spec.whatwg.org/#es-DOMString
n.converters.DOMString=function(A,e,t,r){
// 1. If V is null and the conversion is to an IDL type
//    associated with the [LegacyNullToEmptyString]
//    extended attribute, then return the DOMString value
//    that represents the empty string.
if(null===A&&r?.legacyNullToEmptyString)return"";
// 2. Let x be ? ToString(V).
if("symbol"==typeof A)throw n.errors.exception({header:e,message:`${t} is a symbol, which cannot be converted to a DOMString.`});
// 3. Return the IDL DOMString value that represents the
//    same sequence of code units as the one the
//    ECMAScript String value x represents.
return String(A)},
// https://webidl.spec.whatwg.org/#es-ByteString
n.converters.ByteString=function(A,e,t){
// 1. Let x be ? ToString(V).
// Note: DOMString converter perform ? ToString(V)
const r=n.converters.DOMString(A,e,t);
// 2. If the value of any element of x is greater than
//    255, then throw a TypeError.
for(let A=0;A<r.length;A++)if(r.charCodeAt(A)>255)throw new TypeError(`Cannot convert argument to a ByteString because the character at index ${A} has a value of ${r.charCodeAt(A)} which is greater than 255.`);
// 3. Return an IDL ByteString value whose length is the
//    length of x, and where the value of each element is
//    the value of the corresponding element of x.
return r},
// https://webidl.spec.whatwg.org/#es-USVString
// TODO: rewrite this so we can control the errors thrown
n.converters.USVString=r,
// https://webidl.spec.whatwg.org/#es-boolean
n.converters.boolean=function(A){
// 2. Return the IDL boolean value that is the one that represents
//    the same truth value as the ECMAScript Boolean value x.
return Boolean(A)},
// https://webidl.spec.whatwg.org/#es-any
n.converters.any=function(A){return A},
// https://webidl.spec.whatwg.org/#es-long-long
n.converters["long long"]=function(A,e,t){
// 2. Return the IDL long long value that represents
//    the same numeric value as x.
return n.util.ConvertToInt(A,64,"signed",void 0,e,t)},
// https://webidl.spec.whatwg.org/#es-unsigned-long-long
n.converters["unsigned long long"]=function(A,e,t){
// 2. Return the IDL unsigned long long value that
//    represents the same numeric value as x.
return n.util.ConvertToInt(A,64,"unsigned",void 0,e,t)},
// https://webidl.spec.whatwg.org/#es-unsigned-long
n.converters["unsigned long"]=function(A,e,t){
// 2. Return the IDL unsigned long value that
//    represents the same numeric value as x.
return n.util.ConvertToInt(A,32,"unsigned",void 0,e,t)},
// https://webidl.spec.whatwg.org/#es-unsigned-short
n.converters["unsigned short"]=function(A,e,t,r){
// 2. Return the IDL unsigned short value that represents
//    the same numeric value as x.
return n.util.ConvertToInt(A,16,"unsigned",r,e,t)},
// https://webidl.spec.whatwg.org/#idl-ArrayBuffer
n.converters.ArrayBuffer=function(e,t,r,s){
// 1. If Type(V) is not Object, or V does not have an
//    [[ArrayBufferData]] internal slot, then throw a
//    TypeError.
// see: https://tc39.es/ecma262/#sec-properties-of-the-arraybuffer-instances
// see: https://tc39.es/ecma262/#sec-properties-of-the-sharedarraybuffer-instances
if("Object"!==n.util.Type(e)||!A.isAnyArrayBuffer(e))throw n.errors.conversionFailed({prefix:t,argument:`${r} ("${n.util.Stringify(e)}")`,types:["ArrayBuffer"]});
// 2. If the conversion is not to an IDL type associated
//    with the [AllowShared] extended attribute, and
//    IsSharedArrayBuffer(V) is true, then throw a
//    TypeError.
if(!1===s?.allowShared&&A.isSharedArrayBuffer(e))throw n.errors.exception({header:"ArrayBuffer",message:"SharedArrayBuffer is not allowed."});
// 3. If the conversion is not to an IDL type associated
//    with the [AllowResizable] extended attribute, and
//    IsResizableArrayBuffer(V) is true, then throw a
//    TypeError.
if(e.resizable||e.growable)throw n.errors.exception({header:"ArrayBuffer",message:"Received a resizable ArrayBuffer."});
// 4. Return the IDL ArrayBuffer value that is a
//    reference to the same object as V.
return e},n.converters.TypedArray=function(e,t,r,s,o){
// 1. Let T be the IDL type V is being converted to.
// 2. If Type(V) is not Object, or V does not have a
//    [[TypedArrayName]] internal slot with a value
//    equal to T’s name, then throw a TypeError.
if("Object"!==n.util.Type(e)||!A.isTypedArray(e)||e.constructor.name!==t.name)throw n.errors.conversionFailed({prefix:r,argument:`${s} ("${n.util.Stringify(e)}")`,types:[t.name]});
// 3. If the conversion is not to an IDL type associated
//    with the [AllowShared] extended attribute, and
//    IsSharedArrayBuffer(V.[[ViewedArrayBuffer]]) is
//    true, then throw a TypeError.
if(!1===o?.allowShared&&A.isSharedArrayBuffer(e.buffer))throw n.errors.exception({header:"ArrayBuffer",message:"SharedArrayBuffer is not allowed."});
// 4. If the conversion is not to an IDL type associated
//    with the [AllowResizable] extended attribute, and
//    IsResizableArrayBuffer(V.[[ViewedArrayBuffer]]) is
//    true, then throw a TypeError.
if(e.buffer.resizable||e.buffer.growable)throw n.errors.exception({header:"ArrayBuffer",message:"Received a resizable ArrayBuffer."});
// 5. Return the IDL value of type T that is a reference
//    to the same object as V.
return e},n.converters.DataView=function(e,t,r,s){
// 1. If Type(V) is not Object, or V does not have a
//    [[DataView]] internal slot, then throw a TypeError.
if("Object"!==n.util.Type(e)||!A.isDataView(e))throw n.errors.exception({header:t,message:`${r} is not a DataView.`});
// 2. If the conversion is not to an IDL type associated
//    with the [AllowShared] extended attribute, and
//    IsSharedArrayBuffer(V.[[ViewedArrayBuffer]]) is true,
//    then throw a TypeError.
if(!1===s?.allowShared&&A.isSharedArrayBuffer(e.buffer))throw n.errors.exception({header:"ArrayBuffer",message:"SharedArrayBuffer is not allowed."});
// 3. If the conversion is not to an IDL type associated
//    with the [AllowResizable] extended attribute, and
//    IsResizableArrayBuffer(V.[[ViewedArrayBuffer]]) is
//    true, then throw a TypeError.
if(e.buffer.resizable||e.buffer.growable)throw n.errors.exception({header:"ArrayBuffer",message:"Received a resizable ArrayBuffer."});
// 4. Return the IDL DataView value that is a reference
//    to the same object as V.
return e},
// https://webidl.spec.whatwg.org/#BufferSource
n.converters.BufferSource=function(e,t,r,s){if(A.isAnyArrayBuffer(e))return n.converters.ArrayBuffer(e,t,r,{...s,allowShared:!1});if(A.isTypedArray(e))return n.converters.TypedArray(e,e.constructor,t,r,{...s,allowShared:!1});if(A.isDataView(e))return n.converters.DataView(e,t,r,{...s,allowShared:!1});throw n.errors.conversionFailed({prefix:t,argument:`${r} ("${n.util.Stringify(e)}")`,types:["BufferSource"]})},n.converters["sequence<ByteString>"]=n.sequenceConverter(n.converters.ByteString),n.converters["sequence<sequence<ByteString>>"]=n.sequenceConverter(n.converters["sequence<ByteString>"]),n.converters["record<ByteString, ByteString>"]=n.recordConverter(n.converters.ByteString,n.converters.ByteString),LA={webidl:n}}function Me(){if(YA)return GA;YA=1;const{Transform:A}=I,e=w,{redirectStatusSet:t,referrerPolicySet:r,badPortsSet:n}=be(),{getGlobalOrigin:s}=Fe(),{collectASequenceOfCodePoints:o,collectAnHTTPQuotedString:i,removeChars:a,parseMIMEType:g}=Se(),{performance:c}=y,{isBlobLike:Q,ReadableStreamFrom:B,isValidHTTPToken:h,normalizedMethodRecordsBase:C}=CA(),l=E,{isUint8Array:u}=D,{webidl:d}=Ne();let f,p=[];
// https://nodejs.org/api/crypto.html#determining-if-crypto-support-is-unavailable
/** @type {import('crypto')} */try{f=require("node:crypto");const A=["sha256","sha384","sha512"];p=f.getHashes().filter(e=>A.includes(e))}catch{}function m(A){
// https://fetch.spec.whatwg.org/#responses
// A response has an associated URL. It is a pointer to the last URL
// in response’s URL list and null if response’s URL list is empty.
const e=A.urlList,t=e.length;return 0===t?null:e[t-1].toString()}
// https://fetch.spec.whatwg.org/#concept-response-location-url
/**
	 * @see https://www.rfc-editor.org/rfc/rfc1738#section-2.2
	 * @param {string} url
	 * @returns {boolean}
	 */
function k(A){for(let e=0;e<A.length;++e){const t=A.charCodeAt(e);if(t>126||// Non-US-ASCII + DEL
t<32)return!1}return!0}
/**
	 * If string contains non-ASCII characters, assumes it's UTF-8 encoded and decodes it.
	 * Since UTF-8 is a superset of ASCII, this will work for ASCII strings as well.
	 * @param {string} value
	 * @returns {string}
	 */
/** @returns {URL} */
function R(A){return A.urlList[A.urlList.length-1]}
/**
	 * @see https://fetch.spec.whatwg.org/#header-name
	 * @param {string} potentialValue
	 */
const b=h;
/**
	 * @see https://fetch.spec.whatwg.org/#header-value
	 * @param {string} potentialValue
	 */function F(A){
// - Has no leading or trailing HTTP tab or space bytes.
// - Contains no 0x00 (NUL) or HTTP newline bytes.
return!1===("\t"===A[0]||" "===A[0]||"\t"===A[A.length-1]||" "===A[A.length-1]||A.includes("\n")||A.includes("\r")||A.includes("\0"))}
// https://w3c.github.io/webappsec-referrer-policy/#set-requests-referrer-policy-on-redirect
// https://w3c.github.io/hr-time/#dfn-coarsen-time
function S(A,e){
// TODO
return A}
// https://fetch.spec.whatwg.org/#clamp-and-coarsen-connection-timing-info
// https://html.spec.whatwg.org/multipage/origin.html#policy-container
function N(){
// Note: the fetch spec doesn't make use of embedder policy or CSP list
return{referrerPolicy:"strict-origin-when-cross-origin"}}
// https://html.spec.whatwg.org/multipage/origin.html#clone-a-policy-container
/**
	 * @see https://w3c.github.io/webappsec-referrer-policy/#strip-url
	 * @param {URL} url
	 * @param {boolean|undefined} originOnly
	 */
function M(A,e){
// 2. If url’s scheme is a local scheme, then return no referrer.
// 1. Assert: url is a URL.
return l(A instanceof URL),"file:"===(A=new URL(A)).protocol||"about:"===A.protocol||"blank:"===A.protocol?"no-referrer":(
// 3. Set url’s username to the empty string.
A.username="",
// 4. Set url’s password to the empty string.
A.password="",
// 5. Set url’s fragment to null.
A.hash="",
// 6. If the origin-only flag is true, then:
e&&(
// 1. Set url’s path to « the empty string ».
A.pathname="",
// 2. Set url’s query to null.
A.search=""),A)}function U(A){return A instanceof URL&&(
// If child of about, return true
"about:blank"===A.href||"about:srcdoc"===A.href||(
// If scheme is data, return true
"data:"===A.protocol||(
// If file, return true
"file:"===A.protocol||function(A){
// If origin is explicitly null, return false
if(null==A||"null"===A)return!1;const e=new URL(A);
// If secure, return true
if("https:"===e.protocol||"wss:"===e.protocol)return!0;
// If localhost or variants, return true
if(/^127(?:\.[0-9]+){0,2}\.[0-9]+$|^\[(?:0*:)*?:?0*1\]$/.test(e.hostname)||"localhost"===e.hostname||e.hostname.includes("localhost.")||e.hostname.endsWith(".localhost"))return!0;
// If any other, return false
return!1}(A.origin))))}
/**
	 * @see https://w3c.github.io/webappsec-subresource-integrity/#does-response-match-metadatalist
	 * @param {Uint8Array} bytes
	 * @param {string} metadataList
	 */
// https://w3c.github.io/webappsec-subresource-integrity/#grammardef-hash-with-options
// https://www.w3.org/TR/CSP2/#source-list-syntax
// https://www.rfc-editor.org/rfc/rfc5234#appendix-B.1
const L=/(?<algo>sha256|sha384|sha512)-((?<hash>[A-Za-z0-9+/]+|[A-Za-z0-9_-]+)={0,2}(?:\s|$)( +[!-~]*)?)?/i;
/**
	 * @see https://w3c.github.io/webappsec-subresource-integrity/#parse-metadata
	 * @param {string} metadata
	 */function T(A){
// 1. Let result be the empty set.
/** @type {{ algo: string, hash: string }[]} */
const e=[];
// 2. Let empty be equal to true.
let t=!0;
// 3. For each token returned by splitting metadata on spaces:
for(const r of A.split(" ")){
// 1. Set empty to false.
t=!1;
// 2. Parse token as a hash-with-options.
const A=L.exec(r);
// 3. If token does not parse, continue to the next token.
if(null===A||void 0===A.groups||void 0===A.groups.algo)
// Note: Chromium blocks the request at this point, but Firefox
// gives a warning that an invalid integrity was given. The
// correct behavior is to ignore these, and subsequently not
// check the integrity of the resource.
continue;
// 4. Let algorithm be the hash-algo component of token.
const n=A.groups.algo.toLowerCase();
// 5. If algorithm is a hash function recognized by the user
//    agent, add the parsed token to result.
p.includes(n)&&e.push(A.groups)}
// 4. Return no metadata if empty is true, otherwise return result.
return!0===t?"no metadata":e}
/**
	 * @param {{ algo: 'sha256' | 'sha384' | 'sha512' }[]} metadataList
	 */
/**
	 * Compares two base64 strings, allowing for base64url
	 * in the second string.
	 *
	* @param {string} actualValue always base64
	 * @param {string} expectedValue base64 or base64url
	 * @returns {boolean}
	 */
function G(A,e){if(A.length!==e.length)return!1;for(let t=0;t<A.length;++t)if(A[t]!==e[t]){if("+"===A[t]&&"-"===e[t]||"/"===A[t]&&"_"===e[t])continue;return!1}return!0}
// https://w3c.github.io/webappsec-upgrade-insecure-requests/#upgrade-request
/**
	 * @link {https://html.spec.whatwg.org/multipage/origin.html#same-origin}
	 * @param {URL} A
	 * @param {URL} B
	 */
function Y(A,e){
// 1. If A and B are the same opaque origin, then return true.
return A.origin===e.origin&&"null"===A.origin||A.protocol===e.protocol&&A.hostname===e.hostname&&A.port===e.port;
// 2. If A and B are both tuple origins and their schemes,
//    hosts, and port are identical, then return true.
}
// https://tc39.es/ecma262/#sec-%25iteratorprototype%25-object
const v=Object.getPrototypeOf(Object.getPrototypeOf([][Symbol.iterator]()));
/**
	 * @see https://webidl.spec.whatwg.org/#dfn-iterator-prototype-object
	 * @param {string} name name of the instance
	 * @param {symbol} kInternalIterator
	 * @param {string | number} [keyIndex]
	 * @param {string | number} [valueIndex]
	 */function J(A,e,t=0,r=1){class n{
/** @type {any} */
#t;
/** @type {'key' | 'value' | 'key+value'} */#r;
/** @type {number} */#n;
/**
	     * @see https://webidl.spec.whatwg.org/#dfn-default-iterator-object
	     * @param {unknown} target
	     * @param {'key' | 'value' | 'key+value'} kind
	     */constructor(A,e){this.#t=A,this.#r=e,this.#n=0}next(){
// 1. Let interface be the interface for which the iterator prototype object exists.
// 2. Let thisValue be the this value.
// 3. Let object be ? ToObject(thisValue).
// 4. If object is a platform object, then perform a security
//    check, passing:
// 5. If object is not a default iterator object for interface,
//    then throw a TypeError.
if("object"!=typeof this||null===this||!(#t in this))throw new TypeError(`'next' called on an object that does not implement interface ${A} Iterator.`);
// 6. Let index be object’s index.
// 7. Let kind be object’s kind.
// 8. Let values be object’s target's value pairs to iterate over.
const n=this.#n,s=this.#t[e];
// 10. If index is greater than or equal to len, then return
//     CreateIterResultObject(undefined, true).
if(n>=s.length)return{value:void 0,done:!0};
// 11. Let pair be the entry in values at index index.
const{[t]:o,[r]:i}=s[n];
// 12. Set object’s index to index + 1.
// 13. Return the iterator result for pair and kind.
// https://webidl.spec.whatwg.org/#iterator-result
// 1. Let result be a value determined by the value of kind:
let a;switch(this.#n=n+1,this.#r){case"key":
// 1. Let idlKey be pair’s key.
// 2. Let key be the result of converting idlKey to an
//    ECMAScript value.
// 3. result is key.
a=o;break;case"value":
// 1. Let idlValue be pair’s value.
// 2. Let value be the result of converting idlValue to
//    an ECMAScript value.
// 3. result is value.
a=i;break;case"key+value":
// 1. Let idlKey be pair’s key.
// 2. Let idlValue be pair’s value.
// 3. Let key be the result of converting idlKey to an
//    ECMAScript value.
// 4. Let value be the result of converting idlValue to
//    an ECMAScript value.
// 5. Let array be ! ArrayCreate(2).
// 6. Call ! CreateDataProperty(array, "0", key).
// 7. Call ! CreateDataProperty(array, "1", value).
// 8. result is array.
a=[o,i]}
// 2. Return CreateIterResultObject(result, false).
return{value:a,done:!1}}}
// https://webidl.spec.whatwg.org/#dfn-iterator-prototype-object
// @ts-ignore
/**
	   * @param {unknown} target
	   * @param {'key' | 'value' | 'key+value'} kind
	   * @returns {IterableIterator<any>}
	   */
return delete n.prototype.constructor,Object.setPrototypeOf(n.prototype,v),Object.defineProperties(n.prototype,{[Symbol.toStringTag]:{writable:!1,enumerable:!1,configurable:!0,value:`${A} Iterator`},next:{writable:!0,enumerable:!0,configurable:!0}}),function(A,e){return new n(A,e)}}
/**
	 * @see https://webidl.spec.whatwg.org/#dfn-iterator-prototype-object
	 * @param {string} name name of the instance
	 * @param {any} object class
	 * @param {symbol} kInternalIterator
	 * @param {string | number} [keyIndex]
	 * @param {string | number} [valueIndex]
	 */const H=/[^\x00-\xFF]/;// eslint-disable-line
/**
	 * @see https://infra.spec.whatwg.org/#isomorphic-encode
	 * @param {string} input
	 */function V(A){
// 2. Return a byte sequence whose length is equal to input’s code
//    point length and whose bytes have the same values as the
//    values of input’s code points, in the same order
// 1. Assert: input contains no code points greater than U+00FF.
return l(!H.test(A)),A}
/**
	 * @see https://streams.spec.whatwg.org/#readablestreamdefaultreader-read-all-bytes
	 * @see https://streams.spec.whatwg.org/#read-loop
	 * @param {ReadableStreamDefaultReader} reader
	 */async function x(A){const e=[];let t=0;for(;;){const{done:r,value:n}=await A.read();if(r)
// 1. Call successSteps with bytes.
return Buffer.concat(e,t);
// 1. If chunk is not a Uint8Array object, call failureSteps
//    with a TypeError and abort these steps.
if(!u(n))throw new TypeError("Received non-Uint8Array chunk");
// 2. Append the bytes represented by chunk to bytes.
e.push(n),t+=n.length}}
/**
	 * @see https://fetch.spec.whatwg.org/#is-local
	 * @param {URL} url
	 */
/**
	 * @param {string|URL} url
	 * @returns {boolean}
	 */
function W(A){return"string"==typeof A&&":"===A[5]&&"h"===A[0]&&"t"===A[1]&&"t"===A[2]&&"p"===A[3]&&"s"===A[4]||"https:"===A.protocol}
/**
	 * @see https://fetch.spec.whatwg.org/#http-scheme
	 * @param {URL} url
	 */function O(A){l("protocol"in A);// ensure it's a url object
const e=A.protocol;return"http:"===e||"https:"===e}
/**
	 * @see https://fetch.spec.whatwg.org/#simple-range-header-value
	 * @param {string} value
	 * @param {boolean} allowWhitespace
	 */
// A Stream, which pipes the response to zlib.createInflate() or
// zlib.createInflateRaw() depending on the first byte of the Buffer.
// If the lower byte of the first byte is 0x08, then the stream is
// interpreted as a zlib stream, otherwise it's interpreted as a
// raw deflate stream.
class P extends A{#s;
/** @param {zlib.ZlibOptions} [zlibOptions] */constructor(A){super(),this.#s=A}_transform(A,t,r){if(!this._inflateStream){if(0===A.length)return void r();this._inflateStream=8==(15&A[0])?e.createInflate(this.#s):e.createInflateRaw(this.#s),this._inflateStream.on("data",this.push.bind(this)),this._inflateStream.on("end",()=>this.push(null)),this._inflateStream.on("error",A=>this.destroy(A))}this._inflateStream.write(A,t,r)}_final(A){this._inflateStream&&(this._inflateStream.end(),this._inflateStream=null),A()}}
/**
	 * @param {zlib.ZlibOptions} [zlibOptions]
	 * @returns {InflateStream}
	 */
/**
	 * @see https://fetch.spec.whatwg.org/#concept-header-list-get-decode-split
	 * @param {string} name lowercase header name
	 * @param {import('./headers').HeadersList} list
	 */
function q(A,e){
// 1. Let value be the result of getting name from list.
const t=e.get(A,!0);
// 2. If value is null, then return null.
return null===t?null:
/**
	 * @see https://fetch.spec.whatwg.org/#header-value-get-decode-and-split
	 * @param {string|null} value
	 */
function(A){
// 1. Let input be the result of isomorphic decoding value.
const e=A,t={position:0},r=[];
// 2. Let position be a position variable for input, initially pointing at the start of input.
// 4. Let temporaryValue be the empty string.
let n="";
// 5. While position is not past the end of input:
for(;t.position<e.length;){
// 5.2. If position is not past the end of input, then:
if(
// 5.1. Append the result of collecting a sequence of code points that are not U+0022 (")
//      or U+002C (,) from input, given position, to temporaryValue.
n+=o(A=>'"'!==A&&","!==A,e,t),t.position<e.length)
// 5.2.1. If the code point at position within input is U+0022 ("), then:
if(34===e.charCodeAt(t.position)){
// 5.2.1.2. If position is not past the end of input, then continue.
if(
// 5.2.1.1. Append the result of collecting an HTTP quoted string from input, given position, to temporaryValue.
n+=i(e,t),t.position<e.length)continue}else
// 5.2.2. Otherwise:
// 5.2.2.1. Assert: the code point at position within input is U+002C (,).
l(44===e.charCodeAt(t.position)),
// 5.2.2.2. Advance position by 1.
t.position++;
// 5.3. Remove all HTTP tab or space from the start and end of temporaryValue.
n=a(n,!0,!0,A=>9===A||32===A),
// 5.4. Append temporaryValue to values.
r.push(n),
// 5.6. Set temporaryValue to the empty string.
n=""}
// 6. Return values.
return r}(t);
// 3. Return the result of getting, decoding, and splitting value.
}const Z=new TextDecoder;
/**
	 * @see https://encoding.spec.whatwg.org/#utf-8-decode
	 * @param {Buffer} buffer
	 */class z{get baseUrl(){return s()}get origin(){return this.baseUrl?.origin}policyContainer={referrerPolicy:"strict-origin-when-cross-origin"}}const K=new class{settingsObject=new z};return GA={isAborted:function(A){return"aborted"===A.controller.state},isCancelled:function(A){return"aborted"===A.controller.state||"terminated"===A.controller.state}
/**
	 * @see https://fetch.spec.whatwg.org/#concept-method-normalize
	 * @param {string} method
	 */,isValidEncodedURL:k,createDeferredPromise:function(){let A,e;return{promise:new Promise((t,r)=>{A=t,e=r}),resolve:A,reject:e}},ReadableStreamFrom:B,tryUpgradeRequestToAPotentiallyTrustworthyURL:function(A){
// TODO
},clampAndCoarsenConnectionTimingInfo:function(A,e,t){return!A?.startTime||A.startTime<e?{domainLookupStartTime:e,domainLookupEndTime:e,connectionStartTime:e,connectionEndTime:e,secureConnectionStartTime:e,ALPNNegotiatedProtocol:A?.ALPNNegotiatedProtocol}:{domainLookupStartTime:S(A.domainLookupStartTime),domainLookupEndTime:S(A.domainLookupEndTime),connectionStartTime:S(A.connectionStartTime),connectionEndTime:S(A.connectionEndTime),secureConnectionStartTime:S(A.secureConnectionStartTime),ALPNNegotiatedProtocol:A.ALPNNegotiatedProtocol}}
// https://w3c.github.io/hr-time/#dfn-coarsened-shared-current-time
,coarsenedSharedCurrentTime:function(A){return S(c.now())}
// https://fetch.spec.whatwg.org/#create-an-opaque-timing-info
,determineRequestsReferrer:
// https://w3c.github.io/webappsec-referrer-policy/#determine-requests-referrer
function(A){
// 1. Let policy be request's referrer policy.
const e=A.referrerPolicy;
// Note: policy cannot (shouldn't) be null or an empty string.
l(e);
// 2. Let environment be request’s client.
let t=null;
// 3. Switch on request’s referrer:
if("client"===A.referrer){
// Note: node isn't a browser and doesn't implement document/iframes,
// so we bypass this step and replace it with our own.
const A=s();if(!A||"null"===A.origin)return"no-referrer";
// note: we need to clone it as it's mutated
t=new URL(A)}else A.referrer instanceof URL&&(
// Let referrerSource be request’s referrer.
t=A.referrer);
// 4. Let request’s referrerURL be the result of stripping referrerSource for
//    use as a referrer.
let r=M(t);
// 5. Let referrerOrigin be the result of stripping referrerSource for use as
//    a referrer, with the origin-only flag set to true.
const n=M(t,!0);
// 6. If the result of serializing referrerURL is a string whose length is
//    greater than 4096, set referrerURL to referrerOrigin.
r.toString().length>4096&&(r=n);const o=Y(A,r),i=U(r)&&!U(A.url);
// 8. Execute the switch statements corresponding to the value of policy:
switch(e){case"origin":return null!=n?n:M(t,!0);case"unsafe-url":return r;case"same-origin":return o?n:"no-referrer";case"origin-when-cross-origin":return o?r:n;case"strict-origin-when-cross-origin":{const e=R(A);
// 1. If the origin of referrerURL and the origin of request’s current
//    URL are the same, then return referrerURL.
return Y(r,e)?r:
// 2. If referrerURL is a potentially trustworthy URL and request’s
//    current URL is not a potentially trustworthy URL, then return no
//    referrer.
U(r)&&!U(e)?"no-referrer":n}// eslint-disable-line
/**
	       * 1. If referrerURL is a potentially trustworthy URL and
	       * request’s current URL is not a potentially trustworthy URL,
	       * then return no referrer.
	       * 2. Return referrerOrigin
	      */
default:// eslint-disable-line
return i?"no-referrer":n}},makePolicyContainer:N,clonePolicyContainer:function(A){return{referrerPolicy:A.referrerPolicy}},appendFetchMetadata:function(A){
//  https://w3c.github.io/webappsec-fetch-metadata/#sec-fetch-dest-header
//  TODO
//  https://w3c.github.io/webappsec-fetch-metadata/#sec-fetch-mode-header
//  1. Assert: r’s url is a potentially trustworthy URL.
//  TODO
//  2. Let header be a Structured Header whose value is a token.
let e=null;
//  3. Set header’s value to r’s mode.
e=A.mode,
//  4. Set a structured field value `Sec-Fetch-Mode`/header in r’s header list.
A.headersList.set("sec-fetch-mode",e,!0)}
// https://fetch.spec.whatwg.org/#append-a-request-origin-header
,appendRequestOriginHeader:function(A){
// 1. Let serializedOrigin be the result of byte-serializing a request origin
//    with request.
// TODO: implement "byte-serializing a request origin"
let e=A.origin;
// - "'client' is changed to an origin during fetching."
//   This doesn't happen in undici (in most cases) because undici, by default,
//   has no concept of origin.
// - request.origin can also be set to request.client.origin (client being
//   an environment settings object), which is undefined without using
//   setGlobalOrigin.
if("client"!==e&&void 0!==e)
// 2. If request’s response tainting is "cors" or request’s mode is "websocket",
//    then append (`Origin`, serializedOrigin) to request’s header list.
// 3. Otherwise, if request’s method is neither `GET` nor `HEAD`, then:
if("cors"===A.responseTainting||"websocket"===A.mode)A.headersList.append("origin",e,!0);else if("GET"!==A.method&&"HEAD"!==A.method){
// 1. Switch on request’s referrer policy:
switch(A.referrerPolicy){case"no-referrer":
// Set serializedOrigin to `null`.
e=null;break;case"no-referrer-when-downgrade":case"strict-origin":case"strict-origin-when-cross-origin":
// If request’s origin is a tuple origin, its scheme is "https", and
// request’s current URL’s scheme is not "https", then set
// serializedOrigin to `null`.
A.origin&&W(A.origin)&&!W(R(A))&&(e=null);break;case"same-origin":
// If request’s origin is not same origin with request’s current URL’s
// origin, then set serializedOrigin to `null`.
Y(A,R(A))||(e=null);
// Do nothing.
}
// 2. Append (`Origin`, serializedOrigin) to request’s header list.
A.headersList.append("origin",e,!0)}},TAOCheck:
// https://fetch.spec.whatwg.org/#concept-tao-check
function(){
// TODO
return"success"},corsCheck:
// https://fetch.spec.whatwg.org/#concept-cors-check
function(){
// TODO
return"success"},crossOriginResourcePolicyCheck:
// https://fetch.spec.whatwg.org/#cross-origin-resource-policy-check
function(){
// TODO
return"allowed"},createOpaqueTimingInfo:function(A){return{startTime:A.startTime??0,redirectStartTime:0,redirectEndTime:0,postRedirectStartTime:A.startTime??0,finalServiceWorkerStartTime:0,finalNetworkResponseStartTime:0,finalNetworkRequestStartTime:0,endTime:0,encodedBodySize:0,decodedBodySize:0,finalConnectionTimingInfo:null}},setRequestReferrerPolicyOnRedirect:function(A,e){
//  Given a request request and a response actualResponse, this algorithm
//  updates request’s referrer policy according to the Referrer-Policy
//  header (if any) in actualResponse.
// 1. Let policy be the result of executing § 8.1 Parse a referrer policy
// from a Referrer-Policy header on actualResponse.
// 8.1 Parse a referrer policy from a Referrer-Policy header
// 1. Let policy-tokens be the result of extracting header list values given `Referrer-Policy` and response’s header list.
const{headersList:t}=e,n=(t.get("referrer-policy",!0)??"").split(",");
// 2. Let policy be the empty string.
// 3. For each token in policy-tokens, if token is a referrer policy and token is not the empty string, then set policy to token.
// 4. Return policy.
// Note: As the referrer-policy can contain multiple policies
// separated by comma, we need to loop through all of them
// and pick the first valid one.
// Ref: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Referrer-Policy#specify_a_fallback_policy
let s="";if(n.length>0)
// The right-most policy takes precedence.
// The left-most policy is the fallback.
for(let A=n.length;0!==A;A--){const e=n[A-1].trim();if(r.has(e)){s=e;break}}
// 2. If policy is not the empty string, then set request’s referrer policy to policy.
""!==s&&(A.referrerPolicy=s)},isValidHTTPToken:h,requestBadPort:function(A){
// 1. Let url be request’s current URL.
const e=R(A);
// 2. If url’s scheme is an HTTP(S) scheme and url’s port is a bad port,
// then return blocked.
return O(e)&&n.has(e.port)?"blocked":"allowed";
// 3. Return allowed.
},requestCurrentURL:R,responseURL:m,responseLocationURL:function(A,e){
// 1. If response’s status is not a redirect status, then return null.
if(!t.has(A.status))return null;
// 2. Let location be the result of extracting header list values given
// `Location` and response’s header list.
let r=A.headersList.get("location",!0);
// 3. If location is a header value, then set location to the result of
//    parsing location with response’s URL.
var n;
// 5. Return location.
return null!==r&&F(r)&&(k(r)||(
// Some websites respond location header in UTF-8 form without encoding them as ASCII
// and major browsers redirect them to correctly UTF-8 encoded addresses.
// Here, we handle that behavior in the same way.
n=r,r=Buffer.from(n,"binary").toString("utf8")),r=new URL(r,m(A))),
// 4. If location is a URL whose fragment is null, then set location’s
// fragment to requestFragment.
r&&!r.hash&&(r.hash=e),r},isBlobLike:Q,isURLPotentiallyTrustworthy:U,isValidReasonPhrase:
// Check whether |statusText| is a ByteString and
// matches the Reason-Phrase token production.
// RFC 2616: https://tools.ietf.org/html/rfc2616
// RFC 7230: https://tools.ietf.org/html/rfc7230
// "reason-phrase = *( HTAB / SP / VCHAR / obs-text )"
// https://github.com/chromium/chromium/blob/94.0.4604.1/third_party/blink/renderer/core/fetch/response.cc#L116
function(A){for(let e=0;e<A.length;++e){const t=A.charCodeAt(e);if(!(9===t||// HTAB
t>=32&&t<=126||// SP / VCHAR
t>=128&&t<=255))return!1}return!0},sameOrigin:Y,normalizeMethod:function(A){return C[A.toLowerCase()]??A}
// https://infra.spec.whatwg.org/#serialize-a-javascript-value-to-a-json-string
,serializeJavascriptValueToJSONString:function(A){
// 1. Let result be ? Call(%JSON.stringify%, undefined, « value »).
const e=JSON.stringify(A);
// 2. If result is undefined, then throw a TypeError.
if(void 0===e)throw new TypeError("Value is not JSON serializable");
// 3. Assert: result is a string.
// 4. Return result.
return l("string"==typeof e),e},iteratorMixin:function(A,e,t,r=0,n=1){const s=J(A,t,r,n),o={keys:{writable:!0,enumerable:!0,configurable:!0,value:function(){return d.brandCheck(this,e),s(this,"key")}},values:{writable:!0,enumerable:!0,configurable:!0,value:function(){return d.brandCheck(this,e),s(this,"value")}},entries:{writable:!0,enumerable:!0,configurable:!0,value:function(){return d.brandCheck(this,e),s(this,"key+value")}},forEach:{writable:!0,enumerable:!0,configurable:!0,value:function(t,r=globalThis){if(d.brandCheck(this,e),d.argumentLengthCheck(arguments,1,`${A}.forEach`),"function"!=typeof t)throw new TypeError(`Failed to execute 'forEach' on '${A}': parameter 1 is not of type 'Function'.`);for(const{0:A,1:e}of s(this,"key+value"))t.call(r,e,A,this)}}};return Object.defineProperties(e.prototype,{...o,[Symbol.iterator]:{writable:!0,enumerable:!1,configurable:!0,value:o.entries.value}})}
/**
	 * @see https://fetch.spec.whatwg.org/#body-fully-read
	 */,createIterator:J,isValidHeaderName:b,isValidHeaderValue:F,isErrorLike:function(A){return A instanceof Error||"Error"===A?.constructor?.name||"DOMException"===A?.constructor?.name},fullyReadBody:async function(A,e,t){
// 1. If taskDestination is null, then set taskDestination to
//    the result of starting a new parallel queue.
// 2. Let successSteps given a byte sequence bytes be to queue a
//    fetch task to run processBody given bytes, with taskDestination.
const r=e,n=t;
// 3. Let errorSteps be to queue a fetch task to run processBodyError,
//    with taskDestination.
// 4. Let reader be the result of getting a reader for body’s stream.
//    If that threw an exception, then run errorSteps with that
//    exception and return.
let s;try{s=A.stream.getReader()}catch(A){return void n(A)}
// 5. Read all bytes from reader, given successSteps and errorSteps.
try{r(await x(s))}catch(A){n(A)}},bytesMatch:function(A,e){
// If node is not built with OpenSSL support, we cannot check
// a request's integrity, so allow it by default (the spec will
// allow requests if an invalid hash is given, as precedence).
/* istanbul ignore if: only if node is built with --without-ssl */
if(void 0===f)return!0;
// 1. Let parsedMetadata be the result of parsing metadataList.
const t=T(e);
// 2. If parsedMetadata is no metadata, return true.
if("no metadata"===t)return!0;
// 3. If response is not eligible for integrity validation, return false.
// TODO
// 4. If parsedMetadata is the empty set, return true.
if(0===t.length)return!0;
// 5. Let metadata be the result of getting the strongest
//    metadata from parsedMetadata.
const r=function(A){
// Let algorithm be the algo component of the first item in metadataList.
// Can be sha256
let e=A[0].algo;
// If the algorithm is sha512, then it is the strongest
// and we can return immediately
if("5"===e[3])return e;for(let t=1;t<A.length;++t){const r=A[t];
// If the algorithm is sha512, then it is the strongest
// and we can break the loop immediately
if("5"===r.algo[3]){e="sha512";break;
// If the algorithm is sha384, then a potential sha256 or sha384 is ignored
}"3"!==e[3]&&("3"===r.algo[3]&&(e="sha384"))}return e}(t),n=function(A,e){if(1===A.length)return A;let t=0;for(let r=0;r<A.length;++r)A[r].algo===e&&(A[t++]=A[r]);return A.length=t,A}(t,r);
// 6. For each item in metadata:
for(const e of n){
// 1. Let algorithm be the alg component of item.
const t=e.algo,r=e.hash;
// 2. Let expectedValue be the val component of item.
// See https://github.com/web-platform-tests/wpt/commit/e4c5cc7a5e48093220528dfdd1c4012dc3837a0e
// "be liberal with padding". This is annoying, and it's not even in the spec.
// 3. Let actualValue be the result of applying algorithm to bytes.
let n=f.createHash(t).update(A).digest("base64");
// 4. If actualValue is a case-sensitive match for expectedValue,
//    return true.
if("="===n[n.length-1]&&(n="="===n[n.length-2]?n.slice(0,-2):n.slice(0,-1)),G(n,r))return!0}
// 7. Return false.
return!1},isReadableStreamLike:function(A){return A instanceof ReadableStream||"ReadableStream"===A[Symbol.toStringTag]&&"function"==typeof A.tee}
/**
	 * @param {ReadableStreamController<Uint8Array>} controller
	 */,readableStreamClose:function(A){try{A.close(),A.byobRequest?.respond(0)}catch(A){
// TODO: add comment explaining why this error occurs.
if(!A.message.includes("Controller is already closed")&&!A.message.includes("ReadableStream is already closed"))throw A}},isomorphicEncode:V,urlIsLocal:function(A){l("protocol"in A);// ensure it's a url object
const e=A.protocol;return"about:"===e||"blob:"===e||"data:"===e},urlHasHttpsScheme:W,urlIsHttpHttpsScheme:O,readAllBytes:x,simpleRangeHeaderValue:function(A,e){
// 1. Let data be the isomorphic decoding of value.
// Note: isomorphic decoding takes a sequence of bytes (ie. a Uint8Array) and turns it into a string,
// nothing more. We obviously don't need to do that if value is a string already.
const t=A;
// 2. If data does not start with "bytes", then return failure.
if(!t.startsWith("bytes"))return"failure";
// 3. Let position be a position variable for data, initially pointing at the 5th code point of data.
const r={position:5};
// 4. If allowWhitespace is true, collect a sequence of code points that are HTTP tab or space,
//    from data given position.
// 5. If the code point at position within data is not U+003D (=), then return failure.
if(e&&o(A=>"\t"===A||" "===A,t,r),61!==t.charCodeAt(r.position))return"failure";
// 6. Advance position by 1.
r.position++,
// 7. If allowWhitespace is true, collect a sequence of code points that are HTTP tab or space, from
//    data given position.
e&&o(A=>"\t"===A||" "===A,t,r);
// 8. Let rangeStart be the result of collecting a sequence of code points that are ASCII digits,
//    from data given position.
const n=o(A=>{const e=A.charCodeAt(0);return e>=48&&e<=57},t,r),s=n.length?Number(n):null;
// 9. Let rangeStartValue be rangeStart, interpreted as decimal number, if rangeStart is not the
//    empty string; otherwise null.
// 11. If the code point at position within data is not U+002D (-), then return failure.
if(
// 10. If allowWhitespace is true, collect a sequence of code points that are HTTP tab or space,
//     from data given position.
e&&o(A=>"\t"===A||" "===A,t,r),45!==t.charCodeAt(r.position))return"failure";
// 12. Advance position by 1.
r.position++,
// 13. If allowWhitespace is true, collect a sequence of code points that are HTTP tab
//     or space, from data given position.
// Note from Khafra: its the same step as in #8 again lol
e&&o(A=>"\t"===A||" "===A,t,r);
// 14. Let rangeEnd be the result of collecting a sequence of code points that are
//     ASCII digits, from data given position.
// Note from Khafra: you wouldn't guess it, but this is also the same step as #8
const i=o(A=>{const e=A.charCodeAt(0);return e>=48&&e<=57},t,r),a=i.length?Number(i):null;
// 15. Let rangeEndValue be rangeEnd, interpreted as decimal number, if rangeEnd
//     is not the empty string; otherwise null.
// Note from Khafra: THE SAME STEP, AGAIN!!!
// Note: why interpret as a decimal if we only collect ascii digits?
// 16. If position is not past the end of data, then return failure.
return r.position<t.length||null===a&&null===s||s>a?"failure":{rangeStartValue:s,rangeEndValue:a};
// 17. If rangeEndValue and rangeStartValue are null, then return failure.
}
/**
	 * @see https://fetch.spec.whatwg.org/#build-a-content-range
	 * @param {number} rangeStart
	 * @param {number} rangeEnd
	 * @param {number} fullLength
	 */,buildContentRange:function(A,e,t){
// 1. Let contentRange be `bytes `.
let r="bytes ";
// 2. Append rangeStart, serialized and isomorphic encoded, to contentRange.
// 7. Return contentRange.
return r+=V(`${A}`),
// 3. Append 0x2D (-) to contentRange.
r+="-",
// 4. Append rangeEnd, serialized and isomorphic encoded to contentRange.
r+=V(`${e}`),
// 5. Append 0x2F (/) to contentRange.
r+="/",
// 6. Append fullLength, serialized and isomorphic encoded to contentRange.
r+=V(`${t}`),r},parseMetadata:T,createInflate:function(A){return new P(A)}
/**
	 * @see https://fetch.spec.whatwg.org/#concept-header-extract-mime-type
	 * @param {import('./headers').HeadersList} headers
	 */,extractMimeType:function(A){
// 1. Let charset be null.
let e=null,t=null,r=null;
// 2. Let essence be null.
// 4. Let values be the result of getting, decoding, and splitting `Content-Type` from headers.
const n=q("content-type",A);
// 5. If values is null, then return failure.
if(null===n)return"failure";
// 6. For each value of values:
for(const A of n){
// 6.1. Let temporaryMimeType be the result of parsing value.
const n=g(A);
// 6.2. If temporaryMimeType is failure or its essence is "*/*", then continue.
"failure"!==n&&"*/*"!==n.essence&&(
// 6.3. Set mimeType to temporaryMimeType.
r=n,
// 6.4. If mimeType’s essence is not essence, then:
r.essence!==t?(
// 6.4.1. Set charset to null.
e=null,
// 6.4.2. If mimeType’s parameters["charset"] exists, then set charset to
//        mimeType’s parameters["charset"].
r.parameters.has("charset")&&(e=r.parameters.get("charset")),
// 6.4.3. Set essence to mimeType’s essence.
t=r.essence):r.parameters.has("charset")||null===e||
// 6.5. Otherwise, if mimeType’s parameters["charset"] does not exist, and
//      charset is non-null, set mimeType’s parameters["charset"] to charset.
r.parameters.set("charset",e))}
// 7. If mimeType is null, then return failure.
return r??"failure";
// 8. Return mimeType.
},getDecodeSplit:q,utf8DecodeBytes:function(A){return 0===A.length?"":(
// 1. Let buffer be the result of peeking three bytes from
//    ioQueue, converted to a byte sequence.
// 2. If buffer is 0xEF 0xBB 0xBF, then read three
//    bytes from ioQueue. (Do nothing with those bytes.)
239===A[0]&&187===A[1]&&191===A[2]&&(A=A.subarray(3)),Z.decode(A))},environmentSettingsObject:K},GA}function Ue(){return JA?vA:(JA=1,vA={kUrl:Symbol("url"),kHeaders:Symbol("headers"),kSignal:Symbol("signal"),kState:Symbol("state"),kDispatcher:Symbol("dispatcher")})}function Le(){if(VA)return HA;VA=1;const{Blob:A,File:e}=C,{kState:t}=Ue(),{webidl:r}=Ne();
// TODO(@KhafraDev): remove
class n{constructor(A,e,r={}){
// TODO: argument idl type check
// The File constructor is invoked with two or three parameters, depending
// on whether the optional dictionary parameter is used. When the File()
// constructor is invoked, user agents must run the following steps:
// 1. Let bytes be the result of processing blob parts given fileBits and
// options.
// 2. Let n be the fileName argument to the constructor.
const n=e,s=r.type,o=r.lastModified??Date.now();
// 3. Process FilePropertyBag dictionary argument by running the following
// substeps:
//    1. If the type member is provided and is not the empty string, let t
//    be set to the type dictionary member. If t contains any characters
//    outside the range U+0020 to U+007E, then set t to the empty string
//    and return from these substeps.
//    TODO
// 4. Return a new File object F such that:
// F refers to the bytes byte sequence.
// F.size is set to the number of total bytes in bytes.
// F.name is set to n.
// F.type is set to t.
// F.lastModified is set to d.
this[t]={blobLike:A,name:n,type:s,lastModified:o}}stream(...A){return r.brandCheck(this,n),this[t].blobLike.stream(...A)}arrayBuffer(...A){return r.brandCheck(this,n),this[t].blobLike.arrayBuffer(...A)}slice(...A){return r.brandCheck(this,n),this[t].blobLike.slice(...A)}text(...A){return r.brandCheck(this,n),this[t].blobLike.text(...A)}get size(){return r.brandCheck(this,n),this[t].blobLike.size}get type(){return r.brandCheck(this,n),this[t].blobLike.type}get name(){return r.brandCheck(this,n),this[t].name}get lastModified(){return r.brandCheck(this,n),this[t].lastModified}get[Symbol.toStringTag](){return"File"}}return r.converters.Blob=r.interfaceConverter(A),HA={FileLike:n,isFileLike:
// If this function is moved to ./util.js, some tools (such as
// rollup) will warn about circular dependencies. See:
// https://github.com/nodejs/undici/issues/1629
function(A){return A instanceof e||A&&("function"==typeof A.stream||"function"==typeof A.arrayBuffer)&&"File"===A[Symbol.toStringTag]}}}function Te(){if(WA)return xA;WA=1;const{isBlobLike:A,iteratorMixin:e}=Me(),{kState:t}=Ue(),{kEnumerableProperty:r}=CA(),{FileLike:n,isFileLike:s}=Le(),{webidl:o}=Ne(),{File:i}=C,a=l,g=globalThis.File??i;
// https://xhr.spec.whatwg.org/#formdata
class c{constructor(A){if(o.util.markAsUncloneable(this),void 0!==A)throw o.errors.conversionFailed({prefix:"FormData constructor",argument:"Argument 1",types:["undefined"]});this[t]=[]}append(e,r,n=void 0){o.brandCheck(this,c);const s="FormData.append";if(o.argumentLengthCheck(arguments,2,s),3===arguments.length&&!A(r))throw new TypeError("Failed to execute 'append' on 'FormData': parameter 2 is not of type 'Blob'");
// 1. Let value be value if given; otherwise blobValue.
// 2. Let entry be the result of creating an entry with
// name, value, and filename if given.
const i=Q(e=o.converters.USVString(e,s,"name"),r=A(r)?o.converters.Blob(r,s,"value",{strict:!1}):o.converters.USVString(r,s,"value"),n=3===arguments.length?o.converters.USVString(n,s,"filename"):void 0);
// 3. Append entry to this’s entry list.
this[t].push(i)}delete(A){o.brandCheck(this,c);const e="FormData.delete";o.argumentLengthCheck(arguments,1,e),A=o.converters.USVString(A,e,"name"),
// The delete(name) method steps are to remove all entries whose name
// is name from this’s entry list.
this[t]=this[t].filter(e=>e.name!==A)}get(A){o.brandCheck(this,c);const e="FormData.get";o.argumentLengthCheck(arguments,1,e),A=o.converters.USVString(A,e,"name");
// 1. If there is no entry whose name is name in this’s entry list,
// then return null.
const r=this[t].findIndex(e=>e.name===A);return-1===r?null:this[t][r].value;
// 2. Return the value of the first entry whose name is name from
// this’s entry list.
}getAll(A){o.brandCheck(this,c);const e="FormData.getAll";
// 1. If there is no entry whose name is name in this’s entry list,
// then return the empty list.
// 2. Return the values of all entries whose name is name, in order,
// from this’s entry list.
return o.argumentLengthCheck(arguments,1,e),A=o.converters.USVString(A,e,"name"),this[t].filter(e=>e.name===A).map(A=>A.value)}has(A){o.brandCheck(this,c);const e="FormData.has";
// The has(name) method steps are to return true if there is an entry
// whose name is name in this’s entry list; otherwise false.
return o.argumentLengthCheck(arguments,1,e),A=o.converters.USVString(A,e,"name"),-1!==this[t].findIndex(e=>e.name===A)}set(e,r,n=void 0){o.brandCheck(this,c);const s="FormData.set";if(o.argumentLengthCheck(arguments,2,s),3===arguments.length&&!A(r))throw new TypeError("Failed to execute 'set' on 'FormData': parameter 2 is not of type 'Blob'");
// The set(name, value) and set(name, blobValue, filename) method steps
// are:
// 1. Let value be value if given; otherwise blobValue.
e=o.converters.USVString(e,s,"name"),r=A(r)?o.converters.Blob(r,s,"name",{strict:!1}):o.converters.USVString(r,s,"name"),n=3===arguments.length?o.converters.USVString(n,s,"name"):void 0;
// 2. Let entry be the result of creating an entry with name, value, and
// filename if given.
const i=Q(e,r,n),a=this[t].findIndex(A=>A.name===e);
// 3. If there are entries in this’s entry list whose name is name, then
// replace the first such entry with entry and remove the others.
-1!==a?this[t]=[...this[t].slice(0,a),i,...this[t].slice(a+1).filter(A=>A.name!==e)]:
// 4. Otherwise, append entry to this’s entry list.
this[t].push(i)}[a.inspect.custom](A,e){const r=this[t].reduce((A,e)=>(A[e.name]?Array.isArray(A[e.name])?A[e.name].push(e.value):A[e.name]=[A[e.name],e.value]:A[e.name]=e.value,A),{__proto__:null});e.depth??=A,e.colors??=!0;const n=a.formatWithOptions(e,r);
// remove [Object null prototype]
return`FormData ${n.slice(n.indexOf("]")+2)}`}}
/**
	 * @see https://html.spec.whatwg.org/multipage/form-control-infrastructure.html#create-an-entry
	 * @param {string} name
	 * @param {string|Blob} value
	 * @param {?string} filename
	 * @returns
	 */
function Q(A,e,t){
// 1. Set name to the result of converting name into a scalar value string.
// Note: This operation was done by the webidl converter USVString.
// 2. If value is a string, then set value to the result of converting
//    value into a scalar value string.
if("string"==typeof e);else
// 2. If filename is given, then set value to a new File object,
//    representing the same bytes, whose name attribute is filename.
if(
// 3. Otherwise:
// 1. If value is not a File object, then set value to a new File object,
//    representing the same bytes, whose name attribute value is "blob"
s(e)||(e=e instanceof Blob?new g([e],"blob",{type:e.type}):new n(e,"blob",{type:e.type})),void 0!==t){
/** @type {FilePropertyBag} */
const A={type:e.type,lastModified:e.lastModified};e=e instanceof i?new g([e],t,A):new n(e,t,A)}
// 4. Return an entry whose name is name and whose value is value.
return{name:A,value:e}}return e("FormData",c,t,"name","value"),Object.defineProperties(c.prototype,{append:r,delete:r,get:r,getAll:r,has:r,set:r,[Symbol.toStringTag]:{value:"FormData",configurable:!0}}),xA={FormData:c,makeEntry:Q}}function Ge(){if(ZA)return qA;ZA=1;const A=CA(),{ReadableStreamFrom:e,isBlobLike:t,isReadableStreamLike:r,readableStreamClose:n,createDeferredPromise:s,fullyReadBody:o,extractMimeType:i,utf8DecodeBytes:a}=Me(),{FormData:g}=Te(),{kState:c}=Ue(),{webidl:Q}=Ne(),{Blob:B}=C,h=E,{isErrored:l,isDisturbed:u}=I,{isArrayBuffer:d}=D,{serializeAMimeType:f}=Se(),{multipartFormDataParser:p}=function(){if(PA)return OA;PA=1;const{isUSVString:A,bufferToLowerCasedHeaderName:e}=CA(),{utf8DecodeBytes:t}=Me(),{HTTP_TOKEN_CODEPOINTS:r,isomorphicDecode:n}=Se(),{isFileLike:s}=Le(),{makeEntry:o}=Te(),i=E,{File:a}=C,g=globalThis.File??a,c=Buffer.from('form-data; name="'),Q=Buffer.from("; filename"),B=Buffer.from("--"),h=Buffer.from("--\r\n");
/**
	 * @param {string} chars
	 */
function I(A){for(let e=0;e<A.length;++e)if(-128&A.charCodeAt(e))return!1;return!0}
/**
	 * @see https://andreubotella.github.io/multipart-form-data/#multipart-form-data-boundary
	 * @param {string} boundary
	 */
/**
	 * @see https://andreubotella.github.io/multipart-form-data/#parse-multipart-form-data-headers
	 * @param {Buffer} input
	 * @param {{ position: number }} position
	 */
function l(A,t){
// 1. Let name, filename and contentType be null.
let s=null,o=null,i=null,a=null;
// 2. While true:
for(;;){
// 2.1. If position points to a sequence of bytes starting with 0x0D 0x0A (CR LF):
if(13===A[t.position]&&10===A[t.position+1])
// 2.1.1. If name is null, return failure.
return null===s?"failure":{name:s,filename:o,contentType:i,encoding:a};
// 2.1.2. Return name, filename and contentType.
// 2.2. Let header name be the result of collecting a sequence of bytes that are
//      not 0x0A (LF), 0x0D (CR) or 0x3A (:), given position.
let g=d(A=>10!==A&&13!==A&&58!==A,A,t);
// 2.3. Remove any HTTP tab or space bytes from the start or end of header name.
// 2.4. If header name does not match the field-name token production, return failure.
if(g=f(g,!0,!0,A=>9===A||32===A),!r.test(g.toString()))return"failure";
// 2.5. If the byte at position is not 0x3A (:), return failure.
if(58!==A[t.position])return"failure";
// 2.6. Advance position by 1.
// 2.8. Byte-lowercase header name and switch on the result:
switch(t.position++,
// 2.7. Collect a sequence of bytes that are HTTP tab or space bytes given position.
//      (Do nothing with those bytes.)
d(A=>32===A||9===A,A,t),e(g)){case"content-disposition":
// 2. If position does not point to a sequence of bytes starting with
//    `form-data; name="`, return failure.
if(
// 1. Set name and filename to null.
s=o=null,!p(A,c,t))return"failure";
// 3. Advance position so it points at the byte after the next 0x22 (")
//    byte (the one in the sequence of bytes matched above).
if(t.position+=17,
// 4. Set name to the result of parsing a multipart/form-data name given
//    input and position, if the result is not failure. Otherwise, return
//    failure.
s=u(A,t),null===s)return"failure";
// 5. If position points to a sequence of bytes starting with `; filename="`:
if(p(A,Q,t)){
// Note: undici also handles filename*
let e=t.position+Q.length;if(42===A[e]&&(t.position+=1,e+=1),61!==A[e]||34!==A[e+1])// ="
return"failure";
// 1. Advance position so it points at the byte after the next 0x22 (") byte
//    (the one in the sequence of bytes matched above).
if(t.position+=12,
// 2. Set filename to the result of parsing a multipart/form-data name given
//    input and position, if the result is not failure. Otherwise, return failure.
o=u(A,t),null===o)return"failure"}break;case"content-type":{
// 1. Let header value be the result of collecting a sequence of bytes that are
//    not 0x0A (LF) or 0x0D (CR), given position.
let e=d(A=>10!==A&&13!==A,A,t);
// 2. Remove any HTTP tab or space bytes from the end of header value.
e=f(e,!1,!0,A=>9===A||32===A),
// 3. Set contentType to the isomorphic decoding of header value.
i=n(e);break}case"content-transfer-encoding":{let e=d(A=>10!==A&&13!==A,A,t);e=f(e,!1,!0,A=>9===A||32===A),a=n(e);break}default:
// Collect a sequence of bytes that are not 0x0A (LF) or 0x0D (CR), given position.
// (Do nothing with those bytes.)
d(A=>10!==A&&13!==A,A,t)}
// 2.9. If position does not point to a sequence of bytes starting with 0x0D 0x0A
//      (CR LF), return failure. Otherwise, advance position by 2 (past the newline).
if(13!==A[t.position]&&10!==A[t.position+1])return"failure";t.position+=2}}
/**
	 * @see https://andreubotella.github.io/multipart-form-data/#parse-a-multipart-form-data-name
	 * @param {Buffer} input
	 * @param {{ position: number }} position
	 */function u(A,e){
// 1. Assert: The byte at (position - 1) is 0x22 (").
i(34===A[e.position-1]);
// 2. Let name be the result of collecting a sequence of bytes that are not 0x0A (LF), 0x0D (CR) or 0x22 ("), given position.
/** @type {string | Buffer} */
let t=d(A=>10!==A&&13!==A&&34!==A,A,e);
// 3. If the byte at position is not 0x22 ("), return failure. Otherwise, advance position by 1.
return 34!==A[e.position]?null:(e.position++,
// 4. Replace any occurrence of the following subsequences in name with the given byte:
// - `%0A`: 0x0A (LF)
// - `%0D`: 0x0D (CR)
// - `%22`: 0x22 (")
t=(new TextDecoder).decode(t).replace(/%0A/gi,"\n").replace(/%0D/gi,"\r").replace(/%22/g,'"'),t)}
/**
	 * @param {(char: number) => boolean} condition
	 * @param {Buffer} input
	 * @param {{ position: number }} position
	 */function d(A,e,t){let r=t.position;for(;r<e.length&&A(e[r]);)++r;return e.subarray(t.position,t.position=r)}
/**
	 * @param {Buffer} buf
	 * @param {boolean} leading
	 * @param {boolean} trailing
	 * @param {(charCode: number) => boolean} predicate
	 * @returns {Buffer}
	 */function f(A,e,t,r){let n=0,s=A.length-1;if(e)for(;n<A.length&&r(A[n]);)n++;for(;s>0&&r(A[s]);)s--;return 0===n&&s===A.length-1?A:A.subarray(n,s+1)}
/**
	 * Checks if {@param buffer} starts with {@param start}
	 * @param {Buffer} buffer
	 * @param {Buffer} start
	 * @param {{ position: number }} position
	 */function p(A,e,t){if(A.length<e.length)return!1;for(let r=0;r<e.length;r++)if(e[r]!==A[t.position+r])return!1;return!0}return OA={multipartFormDataParser:
/**
	 * @see https://andreubotella.github.io/multipart-form-data/#multipart-form-data-parser
	 * @param {Buffer} input
	 * @param {ReturnType<import('./data-url')['parseMIMEType']>} mimeType
	 */
function(e,r){
// 1. Assert: mimeType’s essence is "multipart/form-data".
i("failure"!==r&&"multipart/form-data"===r.essence);const n=r.parameters.get("boundary");
// 2. If mimeType’s parameters["boundary"] does not exist, return failure.
//    Otherwise, let boundary be the result of UTF-8 decoding mimeType’s
//    parameters["boundary"].
if(void 0===n)return"failure";const a=Buffer.from(`--${n}`,"utf8"),c=[],Q={position:0};
// 3. Let entry list be an empty entry list.
// Note: undici addition, allows leading and trailing CRLFs.
for(;13===e[Q.position]&&10===e[Q.position+1];)Q.position+=2;let E=e.length;for(;10===e[E-1]&&13===e[E-2];)E-=2;
// 5. While true:
for(E!==e.length&&(e=e.subarray(0,E));;){
// 5.1. If position points to a sequence of bytes starting with 0x2D 0x2D
//      (`--`) followed by boundary, advance position by 2 + the length of
//      boundary. Otherwise, return failure.
// Note: boundary is padded with 2 dashes already, no need to add 2.
if(!e.subarray(Q.position,Q.position+a.length).equals(a))return"failure";
// 5.2. If position points to the sequence of bytes 0x2D 0x2D 0x0D 0x0A
//      (`--` followed by CR LF) followed by the end of input, return entry list.
// Note: a body does NOT need to end with CRLF. It can end with --.
if(Q.position+=a.length,Q.position===e.length-2&&p(e,B,Q)||Q.position===e.length-4&&p(e,h,Q))return c;
// 5.3. If position does not point to a sequence of bytes starting with 0x0D
//      0x0A (CR LF), return failure.
if(13!==e[Q.position]||10!==e[Q.position+1])return"failure";
// 5.4. Advance position by 2. (This skips past the newline.)
Q.position+=2;
// 5.5. Let name, filename and contentType be the result of parsing
//      multipart/form-data headers on input and position, if the result
//      is not failure. Otherwise, return failure.
const r=l(e,Q);if("failure"===r)return"failure";let n,E,{name:C,filename:u,contentType:d,encoding:f}=r;
// 5.6. Advance position by 2. (This skips past the empty line that marks
//      the end of the headers.)
Q.position+=2;
// 5.8. Body loop: While position is not past the end of input:
// TODO: the steps here are completely wrong
{const A=e.indexOf(a.subarray(2),Q.position);if(-1===A)return"failure";n=e.subarray(Q.position,A-4),Q.position+=n.length,
// Note: position must be advanced by the body's length before being
// decoded, otherwise the parsing will fail.
"base64"===f&&(n=Buffer.from(n.toString(),"base64"))}
// 5.9. If position does not point to a sequence of bytes starting with
//      0x0D 0x0A (CR LF), return failure. Otherwise, advance position by 2.
if(13!==e[Q.position]||10!==e[Q.position+1])return"failure";
// 5.10. If filename is not null:
Q.position+=2,null!==u?(
// 5.10.1. If contentType is null, set contentType to "text/plain".
d??="text/plain",
// 5.10.2. If contentType is not an ASCII string, set contentType to the empty string.
// Note: `buffer.isAscii` can be used at zero-cost, but converting a string to a buffer is a high overhead.
// Content-Type is a relatively small string, so it is faster to use `String#charCodeAt`.
I(d)||(d=""),
// 5.10.3. Let value be a new File object with name filename, type contentType, and body body.
E=new g([n],u,{type:d})):
// 5.11. Otherwise:
// 5.11.1. Let value be the UTF-8 decoding without BOM of body.
E=t(Buffer.from(n)),
// 5.12. Assert: name is a scalar value string and value is either a scalar value string or a File object.
i(A(C)),i("string"==typeof E&&A(E)||s(E)),
// 5.13. Create an entry with name and value, and append it to entry list.
c.push(o(C,E,u))}},validateBoundary:function(A){const e=A.length;
// - its length is greater or equal to 27 and lesser or equal to 70, and
if(e<27||e>70)return!1;
// - it is composed by bytes in the ranges 0x30 to 0x39, 0x41 to 0x5A, or
//   0x61 to 0x7A, inclusive (ASCII alphanumeric), or which are 0x27 ('),
//   0x2D (-) or 0x5F (_).
for(let t=0;t<e;++t){const e=A.charCodeAt(t);if(!(e>=48&&e<=57||e>=65&&e<=90||e>=97&&e<=122||39===e||45===e||95===e))return!1}return!0}},OA}();let w;try{const A=require("node:crypto");w=e=>A.randomInt(0,e)}catch{w=A=>Math.floor(Math.random(A))}const y=new TextEncoder;function m(){}const k=globalThis.FinalizationRegistry&&0!==process.version.indexOf("v18");let R;
// https://fetch.spec.whatwg.org/#concept-bodyinit-extract
function b(s,o=!1){
// 1. Let stream be null.
let i=null;
// 2. If object is a ReadableStream object, then set stream to object.
i=s instanceof ReadableStream?s:t(s)?s.stream():new ReadableStream({async pull(A){const e="string"==typeof g?y.encode(g):g;e.byteLength&&A.enqueue(e),queueMicrotask(()=>n(A))},start(){},type:"bytes"}),
// 5. Assert: stream is a ReadableStream object.
h(r(i));
// 6. Let action be null.
let a=null,g=null,c=null,Q=null;
// 7. Let source be null.
// 10. Switch on object:
if("string"==typeof s)
// Set source to the UTF-8 encoding of object.
// Note: setting source to a Uint8Array here breaks some mocking assumptions.
g=s,
// Set type to `text/plain;charset=UTF-8`.
Q="text/plain;charset=UTF-8";else if(s instanceof URLSearchParams)
// URLSearchParams
// spec says to run application/x-www-form-urlencoded on body.list
// this is implemented in Node.js as apart of an URLSearchParams instance toString method
// See: https://github.com/nodejs/node/blob/e46c680bf2b211bbd52cf959ca17ee98c7f657f5/lib/internal/url.js#L490
// and https://github.com/nodejs/node/blob/e46c680bf2b211bbd52cf959ca17ee98c7f657f5/lib/internal/url.js#L1100
// Set source to the result of running the application/x-www-form-urlencoded serializer with object’s list.
g=s.toString(),
// Set type to `application/x-www-form-urlencoded;charset=UTF-8`.
Q="application/x-www-form-urlencoded;charset=UTF-8";else if(d(s))
// BufferSource/ArrayBuffer
// Set source to a copy of the bytes held by object.
g=new Uint8Array(s.slice());else if(ArrayBuffer.isView(s))
// BufferSource/ArrayBufferView
// Set source to a copy of the bytes held by object.
g=new Uint8Array(s.buffer.slice(s.byteOffset,s.byteOffset+s.byteLength));else if(A.isFormDataLike(s)){const A=`----formdata-undici-0${`${w(1e11)}`.padStart(11,"0")}`,e=`--${A}\r\nContent-Disposition: form-data`
/*! formdata-polyfill. MIT License. Jimmy Wärting <https://jimmy.warting.se/opensource> */,t=A=>A.replace(/\n/g,"%0A").replace(/\r/g,"%0D").replace(/"/g,"%22"),r=A=>A.replace(/\r?\n|\r/g,"\r\n"),n=[],o=new Uint8Array([13,10]);// '\r\n'
c=0;let i=!1;for(const[A,a]of s)if("string"==typeof a){const s=y.encode(e+`; name="${t(r(A))}"`+`\r\n\r\n${r(a)}\r\n`);n.push(s),c+=s.byteLength}else{const s=y.encode(`${e}; name="${t(r(A))}"`+(a.name?`; filename="${t(a.name)}"`:"")+"\r\n"+`Content-Type: ${a.type||"application/octet-stream"}\r\n\r\n`);n.push(s,a,o),"number"==typeof a.size?c+=s.byteLength+a.size+o.byteLength:i=!0}
// CRLF is appended to the body to function with legacy servers and match other implementations.
// https://github.com/curl/curl/blob/3434c6b46e682452973972e8313613dfa58cd690/lib/mime.c#L1029-L1030
// https://github.com/form-data/form-data/issues/63
const E=y.encode(`--${A}--\r\n`);n.push(E),c+=E.byteLength,i&&(c=null),
// Set source to object.
g=s,a=async function*(){for(const A of n)A.stream?yield*A.stream():yield A},
// Set type to `multipart/form-data; boundary=`,
// followed by the multipart/form-data boundary string generated
// by the multipart/form-data encoding algorithm.
Q=`multipart/form-data; boundary=${A}`}else if(t(s))
// Blob
// Set source to object.
g=s,
// Set length to object’s size.
c=s.size,
// If object’s type attribute is not the empty byte sequence, set
// type to its value.
s.type&&(Q=s.type);else if("function"==typeof s[Symbol.asyncIterator]){
// If keepalive is true, then throw a TypeError.
if(o)throw new TypeError("keepalive");
// If object is disturbed or locked, then throw a TypeError.
if(A.isDisturbed(s)||s.locked)throw new TypeError("Response body object should not be disturbed or locked");i=s instanceof ReadableStream?s:e(s)}
// 11. If source is a byte sequence, then set action to a
// step that returns source and length to source’s length.
// 12. If action is non-null, then run these steps in in parallel:
if(("string"==typeof g||A.isBuffer(g))&&(c=Buffer.byteLength(g)),null!=a){
// Run action.
let A;i=new ReadableStream({async start(){A=a(s)[Symbol.asyncIterator]()},async pull(e){const{value:t,done:r}=await A.next();if(r)
// When running action is done, close stream.
queueMicrotask(()=>{e.close(),e.byobRequest?.respond(0)});else
// Whenever one or more bytes are available and stream is not errored,
// enqueue a Uint8Array wrapping an ArrayBuffer containing the available
// bytes into stream.
if(!l(i)){const A=new Uint8Array(t);A.byteLength&&e.enqueue(A)}return e.desiredSize>0},async cancel(e){await A.return()},type:"bytes"})}
// 13. Let body be a body whose stream is stream, source is source,
// and length is length.
// 14. Return (body, type).
return[{stream:i,source:g,length:c},Q]}
// https://fetch.spec.whatwg.org/#bodyinit-safely-extract
/**
	 * @see https://fetch.spec.whatwg.org/#concept-body-consume-body
	 * @param {Response|Request} object
	 * @param {(value: unknown) => unknown} convertBytesToJSValue
	 * @param {Response|Request} instance
	 */
async function F(A,e,t){
// 1. If object is unusable, then return a promise rejected
//    with a TypeError.
if(Q.brandCheck(A,t),S(A))throw new TypeError("Body is unusable: Body has already been read");!function(A){if(A.aborted)throw new DOMException("The operation was aborted.","AbortError")}(A[c]);
// 2. Let promise be a new promise.
const r=s(),n=A=>r.reject(A),i=A=>{try{r.resolve(e(A))}catch(A){n(A)}};
// 3. Let errorSteps given error be to reject promise with error.
// 5. If object’s body is null, then run successSteps with an
//    empty byte sequence.
return null==A[c].body?(i(Buffer.allocUnsafe(0)),r.promise):(
// 6. Otherwise, fully read object’s body given successSteps,
//    errorSteps, and object’s relevant global object.
await o(A[c].body,i,n),r.promise)}
// https://fetch.spec.whatwg.org/#body-unusable
function S(e){const t=e[c].body;
// An object including the Body interface mixin is
// said to be unusable if its body is non-null and
// its body’s stream is disturbed or locked.
return null!=t&&(t.stream.locked||A.isDisturbed(t.stream))}
/**
	 * @see https://infra.spec.whatwg.org/#parse-json-bytes-to-a-javascript-value
	 * @param {Uint8Array} bytes
	 */function N(A){return JSON.parse(a(A))}
/**
	 * @see https://fetch.spec.whatwg.org/#concept-body-mime-type
	 * @param {import('./response').Response|import('./request').Request} requestOrResponse
	 */function M(A){
// 1. Let headers be null.
// 2. If requestOrResponse is a Request object, then set headers to requestOrResponse’s request’s header list.
// 3. Otherwise, set headers to requestOrResponse’s response’s header list.
/** @type {import('./headers').HeadersList} */
const e=A[c].headersList,t=i(e);
// 4. Let mimeType be the result of extracting a MIME type from headers.
// 5. If mimeType is failure, then return null.
return"failure"===t?null:t;
// 6. Return mimeType.
}return k&&(R=new FinalizationRegistry(A=>{const e=A.deref();!e||e.locked||u(e)||l(e)||e.cancel("Response object has been garbage collected").catch(m)})),qA={extractBody:b,safelyExtractBody:function(e,t=!1){
// 2. Return the results of extracting object.
// To safely extract a body and a `Content-Type` value from
// a byte sequence or BodyInit object object, run these steps:
// 1. If object is a ReadableStream object, then:
return e instanceof ReadableStream&&(
// Assert: object is neither disturbed nor locked.
// istanbul ignore next
h(!A.isDisturbed(e),"The body has already been consumed."),
// istanbul ignore next
h(!e.locked,"The stream is locked.")),b(e,t)},cloneBody:function(A,e){
// To clone a body body, run these steps:
// https://fetch.spec.whatwg.org/#concept-body-clone
// 1. Let « out1, out2 » be the result of teeing body’s stream.
const[t,r]=e.stream.tee();
// 2. Set body’s stream to out1.
// 3. Return a body whose stream is out2 and other members are copied from body.
return e.stream=t,{stream:r,length:e.length,source:e.source}},mixinBody:function(A){var e;Object.assign(A.prototype,(e=A,{blob(){
// The blob() method steps are to return the result of
// running consume body with this and the following step
// given a byte sequence bytes: return a Blob whose
// contents are bytes and whose type attribute is this’s
// MIME type.
return F(this,A=>{let e=M(this);
// Return a Blob whose contents are bytes and type attribute
// is mimeType.
return null===e?e="":e&&(e=f(e)),new B([A],{type:e})},e)},arrayBuffer(){
// The arrayBuffer() method steps are to return the result
// of running consume body with this and the following step
// given a byte sequence bytes: return a new ArrayBuffer
// whose contents are bytes.
return F(this,A=>new Uint8Array(A).buffer,e)},text(){
// The text() method steps are to return the result of running
// consume body with this and UTF-8 decode.
return F(this,a,e)},json(){
// The json() method steps are to return the result of running
// consume body with this and parse JSON from bytes.
return F(this,N,e)},formData(){
// The formData() method steps are to return the result of running
// consume body with this and the following step given a byte sequence bytes:
return F(this,A=>{
// 1. Let mimeType be the result of get the MIME type with this.
const e=M(this);
// 2. If mimeType is non-null, then switch on mimeType’s essence and run
//    the corresponding steps:
if(null!==e)switch(e.essence){case"multipart/form-data":{
// 1. ... [long step]
const t=p(A,e);
// 2. If that fails for some reason, then throw a TypeError.
if("failure"===t)throw new TypeError("Failed to parse body as FormData.");
// 3. Return a new FormData object, appending each entry,
//    resulting from the parsing operation, to its entry list.
const r=new g;return r[c]=t,r}case"application/x-www-form-urlencoded":{
// 1. Let entries be the result of parsing bytes.
const e=new URLSearchParams(A.toString()),t=new g;
// 2. If entries is failure, then throw a TypeError.
// 3. Return a new FormData object whose entry list is entries.
for(const[A,r]of e)t.append(A,r);return t}}
// 3. Throw a TypeError.
throw new TypeError('Content-Type was not one of "multipart/form-data" or "application/x-www-form-urlencoded".')},e)},bytes(){
// The bytes() method steps are to return the result of running consume body
// with this and the following step given a byte sequence bytes: return the
// result of creating a Uint8Array from bytes in this’s relevant realm.
return F(this,A=>new Uint8Array(A),e)}}))},streamRegistry:R,hasFinalizationRegistry:k,bodyUnusable:S}}function Ye(){if(KA)return zA;KA=1;
/* global WebAssembly */
const A=E,e=CA(),{channels:t}=lA(),r=fA(),{RequestContentLengthMismatchError:n,ResponseContentLengthMismatchError:s,RequestAbortedError:o,InvalidArgumentError:i,HeadersTimeoutError:a,HeadersOverflowError:g,SocketError:c,InformationalError:Q,BodyTimeoutError:B,HTTPParserError:h,ResponseExceededMaxSizeError:I}=hA(),{kUrl:l,kReset:u,kClient:d,kParser:f,kBlocking:p,kRunning:w,kPending:y,kSize:D,kWriting:m,kQueue:k,kNoRef:R,kKeepAliveDefaultTimeout:b,kHostHeader:F,kPendingIdx:S,kRunningIdx:N,kError:M,kPipelining:U,kSocket:L,kKeepAliveTimeoutValue:T,kMaxHeadersSize:G,kKeepAliveMaxTimeout:Y,kKeepAliveTimeoutThreshold:v,kHeadersTimeout:J,kBodyTimeout:H,kStrictContentLength:V,kMaxRequests:x,kCounter:W,kMaxResponseSize:O,kOnError:P,kResume:q,kHTTPContext:Z}=BA(),z=ke(),K=Buffer.alloc(0),X=Buffer[Symbol.species],_=e.addListener,j=e.removeAllListeners,$=Symbol("kIdleSocketValidation"),AA=Symbol("kIdleSocketValidationTimeout"),eA=Symbol("kSocketUsed");let tA;let rA=null,nA=async function(){const e=process.env.JEST_WORKER_ID?Re():void 0;let t;try{t=await WebAssembly.compile(function(){if(RA)return kA;RA=1;const{Buffer:A}=C;return kA=A.from("AGFzbQEAAAABJwdgAX8Bf2ADf39/AX9gAX8AYAJ/fwBgBH9/f38Bf2AAAGADf39/AALLAQgDZW52GHdhc21fb25faGVhZGVyc19jb21wbGV0ZQAEA2VudhV3YXNtX29uX21lc3NhZ2VfYmVnaW4AAANlbnYLd2FzbV9vbl91cmwAAQNlbnYOd2FzbV9vbl9zdGF0dXMAAQNlbnYUd2FzbV9vbl9oZWFkZXJfZmllbGQAAQNlbnYUd2FzbV9vbl9oZWFkZXJfdmFsdWUAAQNlbnYMd2FzbV9vbl9ib2R5AAEDZW52GHdhc21fb25fbWVzc2FnZV9jb21wbGV0ZQAAAy0sBQYAAAIAAAAAAAACAQIAAgICAAADAAAAAAMDAwMBAQEBAQEBAQEAAAIAAAAEBQFwARISBQMBAAIGCAF/AUGA1AQLB9EFIgZtZW1vcnkCAAtfaW5pdGlhbGl6ZQAIGV9faW5kaXJlY3RfZnVuY3Rpb25fdGFibGUBAAtsbGh0dHBfaW5pdAAJGGxsaHR0cF9zaG91bGRfa2VlcF9hbGl2ZQAvDGxsaHR0cF9hbGxvYwALBm1hbGxvYwAxC2xsaHR0cF9mcmVlAAwEZnJlZQAMD2xsaHR0cF9nZXRfdHlwZQANFWxsaHR0cF9nZXRfaHR0cF9tYWpvcgAOFWxsaHR0cF9nZXRfaHR0cF9taW5vcgAPEWxsaHR0cF9nZXRfbWV0aG9kABAWbGxodHRwX2dldF9zdGF0dXNfY29kZQAREmxsaHR0cF9nZXRfdXBncmFkZQASDGxsaHR0cF9yZXNldAATDmxsaHR0cF9leGVjdXRlABQUbGxodHRwX3NldHRpbmdzX2luaXQAFQ1sbGh0dHBfZmluaXNoABYMbGxodHRwX3BhdXNlABcNbGxodHRwX3Jlc3VtZQAYG2xsaHR0cF9yZXN1bWVfYWZ0ZXJfdXBncmFkZQAZEGxsaHR0cF9nZXRfZXJybm8AGhdsbGh0dHBfZ2V0X2Vycm9yX3JlYXNvbgAbF2xsaHR0cF9zZXRfZXJyb3JfcmVhc29uABwUbGxodHRwX2dldF9lcnJvcl9wb3MAHRFsbGh0dHBfZXJybm9fbmFtZQAeEmxsaHR0cF9tZXRob2RfbmFtZQAfEmxsaHR0cF9zdGF0dXNfbmFtZQAgGmxsaHR0cF9zZXRfbGVu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PgtBhwEMPQtBiAEMPAtBiQEMOwtBigEMOgtBiwEMOQtBjAEMOAtBjQEMNwtBjgEMNgtBjwEMNQtBkAEMNAtBkQEMMwtBkgEMMgtBkwEMMQtBlAEMMAtBlQEMLwtBlgEMLgtBlwEMLQtBmAEMLAtBmQEMKwtBmgEMKgtBmwEMKQtBnAEMKAtBnQEMJwtBngEMJgtBnwEMJQtBoAEMJAtBoQEMIwtBogEMIgtBowEMIQtBpAEMIAtBpQEMHwtBpgEMHgtBpwEMHQtBqAEMHAtBqQEMGwtBqgEMGgtBqwEMGQtBrAEMGAtBrQEMFwtBrgEMFgtBAQwVC0GvAQwUC0GwAQwTC0GxAQwSC0GzAQwRC0GyAQwQC0G0AQwPC0G1AQwOC0G2AQwNC0G3AQwMC0G4AQwLC0G5AQwKC0G6AQwJC0G7AQwIC0HGAQwHC0G8AQwGC0G9AQwFC0G+AQwEC0G/AQwDC0HAAQwCC0HCAQwBC0HBAQshAwNAAkACQAJAAkACQAJAAkACQAJAIAICfwJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJ/AkACQAJAAkACQAJAAkACQAJAAkACQAJAAkAgAgJ/AkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACfwJAAkACfwJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkAC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iQEhAwykAgtBASABLQAAQd8ARw1nGiABQQFqIQEMXgtBigEhAyABIARGDaICIAIoAgAiACAEIAFraiEFIAEgAGtBAWohBgNAIAEtAAAgAEGMzwBqLQAARw1kIABBAUYN+gEgAEEBaiEAIAQgAUEBaiIBRw0ACyACIAU2AgAMogILQYsBIQMgASAERg2hAiACKAIAIgAgBCABa2ohBSABIABrQQJqIQYCQANAIAEtAAAgAEGOzwBqLQAARw1kIABBAkYNASAAQQFqIQAgBCABQQFqIgFHDQALIAIgBTYCAAyiAgsgAkEANgIAIAZBAWohAUECDGULQYwBIQMgASAERg2gAiACKAIAIgAgBCABa2ohBSABIABrQQFqIQYCQANAIAEtAAAgAEHwzwBqLQAARw1jIABBAUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAIgBTYCAAyhAgsgAkEANgIAIAZBAWohAUEfDGQLQY0BIQMgASAERg2fAiACKAIAIgAgBCABa2ohBSABIABrQQFqIQYCQANAIAEtAAAgAEHyzwBqLQAARw1iIABBAUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAIgBTYCAAygAgsgAkEANgIAIAZBAWohAUEJDGMLIAEgBEYEQEGOASEDDJ8CCwJAAkAgAS0AAEHJAGsOBwBiYmJiYgFiCyABQQFqIQFB+AAhAwyGAgsgAUEBaiEBQfkAIQMMhQILQY8BIQMgASAERg2dAiACKAIAIgAgBCABa2ohBSABIABrQQVqIQYCQANAIAEtAAAgAEGRzwBqLQAARw1gIABBBUYNASAAQQFqIQAgBCABQQFqIgFHDQALIAIgBTYCAAyeAgsgAkEANgIAIAZBAWohAUEYDGELQZABIQMgASAERg2cAiACKAIAIgAgBCABa2ohBSABIABrQQJqIQYCQANAIAEtAAAgAEGXzwBqLQAARw1fIABBAkYNASAAQQFqIQAgBCABQQFqIgFHDQALIAIgBTYCAAydAgsgAkEANgIAIAZBAWohAUEXDGALQZEBIQMgASAERg2bAiACKAIAIgAgBCABa2ohBSABIABrQQZqIQYCQANAIAEtAAAgAEGazwBqLQAARw1eIABBBkYNASAAQQFqIQAgBCABQQFqIgFHDQALIAIgBTYCAAycAgsgAkEANgIAIAZBAWohAUEV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JSIARQ0RIAJB5QA2AhwgAiABNgIUIAIgADYCDEEAIQMMtAELIAIoAgQhACACQQA2AgQgAiAAIAEQJSIARQ0gIAJB0wA2AhwgAiABNgIUIAIgADYCDEEAIQMMswELIAIoAgQhACACQQA2AgQgAiAAIAEQJSIARQ0iIAJB0gA2AhwgAiABNgIUIAIgADYCDEEAIQMMsgELIAIoAgQhACACQQA2AgQgAiAAIAEQJSIARQ0OIAJB5QA2AhwgAiABNgIUIAIgADYCDEEAIQMMsQELIAIoAgQhACACQQA2AgQgAiAAIAEQJSIARQ0dIAJB0wA2AhwgAiABNgIUIAIgADYCDEEAIQMMsAELIAIoAgQhACACQQA2AgQgAiAAIAEQJSIARQ0fIAJB0gA2AhwgAiABNgIUIAIgADYCDEEAIQMMrwELIABBP0cNASABQQFqCyEBQQUhAwyUAQtBACEDIAJBADYCHCACIAE2AhQgAkH9EjYCECACQQc2AgwMrAELIAJBADYCHCACIAE2AhQgAkHcCDYCECACQQc2AgxBACEDDKsBCyACKAIEIQAgAkEANgIEIAIgACABECUiAEUNByACQeUANgIcIAIgATYCFCACIAA2AgxBACEDDKoBCyACKAIEIQAgAkEANgIEIAIgACABECUiAEUNFiACQdMANgIcIAIgATYCFCACIAA2AgxBACEDDKkBCyACKAIEIQAgAkEANgIEIAIgACABECUiAEUNGCACQdIANgIcIAIgATYCFCACIAA2AgxBACEDDKgBCyACQQA2AhwgAiABNgIUIAJBxgo2AhAgAkEHNgIMQQAhAwynAQsgAigCBCEAIAJBADYCBCACIAAgARAlIgBFDQMgAkHlADYCHCACIAE2AhQgAiAANgIMQQAhAwymAQsgAigCBCEAIAJBADYCBCACIAAgARAlIgBFDRIgAkHTADYCHCACIAE2AhQgAiAANgIMQQAhAwylAQsgAigCBCEAIAJBADYCBCACIAAgARAlIgBFDRQgAkHSADYCHCACIAE2AhQgAiAANgIMQQAhAwykAQsgAigCBCEAIAJBADYCBCACIAAgARAlIgBFDQAgAkHlADYCHCACIAE2AhQgAiAANgIMQQAhAwyjAQtB1QAhAwyJAQsgAEEVRwRAIAJBADYCHCACIAE2AhQgAkG5DTYCECACQRo2AgxB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AkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAAkACQAJAIABB5ABrDvQDY2IAAWFhYWFhYQIDBAVhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhBgcICQoLDA0OD2FhYWFhEGFhYWFhYWFhYWFhEWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYRITFBUWFxgZGhthYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhHB0eHyAhIiMkJSYnKCkqKywtLi8wMTIzNDU2YTc4OTphYWFhYWFhYTthYWE8YWFhYT0+P2FhYWFhYWFhQGFhQWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYUJDREVGR0hJSktMTU5PUFFSU2FhYWFhYWFhVFVWV1hZWlthXF1hYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFeYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhX2BhC0HhJw8LQaQhDwtByywPC0H+MQ8LQcAkDwtBqyQPC0GNKA8LQeImDwtBgDAPC0G5Lw8LQdckDwtB7x8PC0HhHw8LQfofDwtB8iAPC0GoLw8LQa4yDwtBiDAPC0HsJw8LQYIiDwtBjh0PC0HQLg8LQcojDwtBxTIPC0HfHA8LQdIcDwtBxCAPC0HXIA8LQaIfDwtB7S4PC0GrMA8LQdQlDwtBzC4PC0H6Lg8LQfwrDwtB0jAPC0HxHQ8LQbsgDwtB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AAADAEGUCAsFBAAAAAUAQaQICwkGAAAABwAAAAgAQdwIC4otSW52YWxpZCBjaGFyIGluIHVybCBxdWVyeQBTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX2JvZHkAQ29udGVudC1MZW5ndGggb3ZlcmZsb3cAQ2h1bmsgc2l6ZSBvdmVyZmxvdwBSZXNwb25zZSBvdmVyZmxvdwBJbnZhbGlkIG1ldGhvZCBmb3IgSFRUUC94LnggcmVxdWVzdABJbnZhbGlkIG1ldGhvZCBmb3IgUlRTUC94LnggcmVxdWVzdABFeHBlY3RlZCBTT1VSQ0UgbWV0aG9kIGZvciBJQ0UveC54IHJlcXVlc3QASW52YWxpZCBjaGFyIGluIHVybCBmcmFnbWVudCBzdGFydABFeHBlY3RlZCBkb3QAU3BhbiBjYWxsYmFjayBlcnJvciBpbiBvbl9zdGF0dXMASW52YWxpZCByZXNwb25zZSBzdGF0dXMASW52YWxpZCBjaGFyYWN0ZXIgaW4gY2h1bmsgZXh0ZW5zaW9ucwBVc2VyIGNhbGxiYWNrIGVycm9yAGBvbl9yZXNldGAgY2FsbGJhY2sgZXJyb3IAYG9uX2NodW5rX2hlYWRlcmAgY2FsbGJhY2sgZXJyb3IAYG9uX21lc3NhZ2VfYmVnaW5gIGNhbGxiYWNrIGVycm9yAGBvbl9jaHVua19leHRlbnNpb25fdmFsdWVgIGNhbGxiYWNrIGVycm9yAGBvbl9zdGF0dXNfY29tcGxldGVgIGNhbGxiYWNrIGVycm9yAGBvbl92ZXJzaW9uX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25fdXJsX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25fY2h1bmtfY29tcGxldGVgIGNhbGxiYWNrIGVycm9yAGBvbl9oZWFkZXJfdmFsdWVfY29tcGxldGVgIGNhbGxiYWNrIGVycm9yAGBvbl9tZXNzYWdlX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25fbWV0aG9kX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25faGVhZGVyX2ZpZWxkX2NvbXBsZXRlYCBjYWxsYmFjayBlcnJvcgBgb25fY2h1bmtfZXh0ZW5zaW9uX25hbWVgIGNhbGxiYWNrIGVycm9yAFVuZXhwZWN0ZWQgY2hhciBpbiB1cmwgc2VydmVyAEludmFsaWQgaGVhZGVyIHZhbHVlIGNoYXIASW52YWxpZCBoZWFkZXIgZmllbGQgY2hhcgBTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX3ZlcnNpb24ASW52YWxpZCBtaW5vciB2ZXJzaW9uAEludmFsaWQgbWFqb3IgdmVyc2lvbgBFeHBlY3RlZCBzcGFjZSBhZnRlciB2ZXJzaW9uAEV4cGVjdGVkIENSTEYgYWZ0ZXIgdmVyc2lvbgBJbnZhbGlkIEhUVFAgdmVyc2lvbgBJbnZhbGlkIGhlYWRlciB0b2tlbgBTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX3VybABJbnZhbGlkIGNoYXJhY3RlcnMgaW4gdXJsAFVuZXhwZWN0ZWQgc3RhcnQgY2hhciBpbiB1cmwARG91YmxlIEAgaW4gdXJsAEVtcHR5IENvbnRlbnQtTGVuZ3RoAEludmFsaWQgY2hhcmFjdGVyIGluIENvbnRlbnQtTGVuZ3RoAER1cGxpY2F0ZSBDb250ZW50LUxlbmd0aABJbnZhbGlkIGNoYXIgaW4gdXJsIHBhdGgAQ29udGVudC1MZW5ndGggY2FuJ3QgYmUgcHJlc2VudCB3aXRoIFRyYW5zZmVyLUVuY29kaW5nAEludmFsaWQgY2hhcmFjdGVyIGluIGNodW5rIHNpemUAU3BhbiBjYWxsYmFjayBlcnJvciBpbiBvbl9oZWFkZXJfdmFsdWUAU3BhbiBjYWxsYmFjayBlcnJvciBpbiBvbl9jaHVua19leHRlbnNpb25fdmFsdWUASW52YWxpZCBjaGFyYWN0ZXIgaW4gY2h1bmsgZXh0ZW5zaW9ucyB2YWx1ZQBNaXNzaW5nIGV4cGVjdGVkIExGIGFmdGVyIGhlYWRlciB2YWx1ZQBJbnZhbGlkIGBUcmFuc2Zlci1FbmNvZGluZ2AgaGVhZGVyIHZhbHVlAEludmFsaWQgY2hhcmFjdGVyIGluIGNodW5rIGV4dGVuc2lvbnMgcXVvdGUgdmFsdWUASW52YWxpZCBjaGFyYWN0ZXIgaW4gY2h1bmsgZXh0ZW5zaW9ucyBxdW90ZWQgdmFsdWUAUGF1c2VkIGJ5IG9uX2hlYWRlcnNfY29tcGxldGUASW52YWxpZCBFT0Ygc3RhdGUAb25fcmVzZXQgcGF1c2UAb25fY2h1bmtfaGVhZGVyIHBhdXNlAG9uX21lc3NhZ2VfYmVnaW4gcGF1c2UAb25fY2h1bmtfZXh0ZW5zaW9uX3ZhbHVlIHBhdXNlAG9uX3N0YXR1c19jb21wbGV0ZSBwYXVzZQBvbl92ZXJzaW9uX2NvbXBsZXRlIHBhdXNlAG9uX3VybF9jb21wbGV0ZSBwYXVzZQBvbl9jaHVua19jb21wbGV0ZSBwYXVzZQBvbl9oZWFkZXJfdmFsdWVfY29tcGxldGUgcGF1c2UAb25fbWVzc2FnZV9jb21wbGV0ZSBwYXVzZQBvbl9tZXRob2RfY29tcGxldGUgcGF1c2UAb25faGVhZGVyX2ZpZWxkX2NvbXBsZXRlIHBhdXNlAG9uX2NodW5rX2V4dGVuc2lvbl9uYW1lIHBhdXNlAFVuZXhwZWN0ZWQgc3BhY2UgYWZ0ZXIgc3RhcnQgbGluZQBTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX2NodW5rX2V4dGVuc2lvbl9uYW1lAEludmFsaWQgY2hhcmFjdGVyIGluIGNodW5rIGV4dGVuc2lvbnMgbmFtZQBQYXVzZSBvbiBDT05ORUNUL1VwZ3JhZGUAUGF1c2Ugb24gUFJJL1VwZ3JhZGUARXhwZWN0ZWQgSFRUUC8yIENvbm5lY3Rpb24gUHJlZmFjZQBTcGFuIGNhbGxiYWNrIGVycm9yIGluIG9uX21ldGhvZABFeHBlY3RlZCBzcGFjZSBhZnRlciBtZXRob2QAU3BhbiBjYWxsYmFjayBlcnJvciBpbiBvbl9oZWFkZXJfZmllbGQAUGF1c2VkAEludmFsaWQgd29yZCBlbmNvdW50ZXJlZABJbnZhbGlkIG1ldGhvZCBlbmNvdW50ZXJlZABVbmV4cGVjdGVkIGNoYXIgaW4gdXJsIHNjaGVtYQBSZXF1ZXN0IGhhcyBpbnZhbGlkIGBUcmFuc2Zlci1FbmNvZGluZ2AAU1dJVENIX1BST1hZAFVTRV9QUk9YWQBNS0FDVElWSVRZAFVOUFJPQ0VTU0FCTEVfRU5USVRZAENPUFkATU9WRURfUEVSTUFORU5UTFkAVE9PX0VBUkxZAE5PVElGWQBGQUlMRURfREVQRU5ERU5DWQBCQURfR0FURVdBWQBQTEFZAFBVVABDSEVDS09VVABHQVRFV0FZX1RJTUVPVVQAUkVRVUVTVF9USU1FT1VUAE5FVFdPUktfQ09OTkVDVF9USU1FT1VUAENPTk5FQ1RJT05fVElNRU9VVABMT0dJTl9USU1FT1VUAE5FVFdPUktfUkVBRF9USU1FT1VUAFBPU1QATUlTRElSRUNURURfUkVRVUVTVABDTElFTlRfQ0xPU0VEX1JFUVVFU1QAQ0xJRU5UX0NMT1NFRF9MT0FEX0JBTEFOQ0VEX1JFUVVFU1QAQkFEX1JFUVVFU1QASFRUUF9SRVFVRVNUX1NFTlRfVE9fSFRUUFNfUE9SVABSRVBPUlQASU1fQV9URUFQT1QAUkVTRVRfQ09OVEVOVABOT19DT05URU5UAFBBUlRJQUxfQ09OVEVOVABIUEVfSU5WQUxJRF9DT05TVEFOVABIUEVfQ0JfUkVTRVQAR0VUAEhQRV9TVFJJQ1QAQ09ORkxJQ1QAVEVNUE9SQVJZX1JFRElSRUNUAFBFUk1BTkVOVF9SRURJUkVDVABDT05ORUNUAE1VTFRJX1NUQVRVUwBIUEVfSU5WQUxJRF9TVEFUVVMAVE9PX01BTllfUkVRVUVTVFMARUFSTFlfSElOVFMAVU5BVkFJTEFCTEVfRk9SX0xFR0FMX1JFQVNPTlMAT1BUSU9OUwBTV0lUQ0hJTkdfUFJPVE9DT0xTAFZBUklBTlRfQUxTT19ORUdPVElBVEVTAE1VTFRJUExFX0NIT0lDRVMASU5URVJOQUxfU0VSVkVSX0VSUk9SAFdFQl9TRVJWRVJfVU5LTk9XTl9FUlJPUgBSQUlMR1VOX0VSUk9SAElERU5USVRZX1BST1ZJREVSX0FVVEhFTlRJQ0FUSU9OX0VSUk9SAFNTTF9DRVJUSUZJQ0FURV9FUlJPUgBJTlZBTElEX1hfRk9SV0FSREVEX0ZPUgBTRVRfUEFSQU1FVEVSAEdFVF9QQVJBTUVURVIASFBFX1VTRVIAU0VFX09USEVSAEhQRV9DQl9DSFVOS19IRUFERVIATUtDQUxFTkRBUgBTRVRVUABXRUJfU0VSVkVSX0lTX0RPV04AVEVBUkRPV04ASFBFX0NMT1NFRF9DT05ORUNUSU9OAEhFVVJJU1RJQ19FWFBJUkFUSU9OAERJU0NPTk5FQ1RFRF9PUEVSQVRJT04ATk9OX0FVVEhPUklUQVRJVkVfSU5GT1JNQVRJT04ASFBFX0lOVkFMSURfVkVSU0lPTgBIUEVfQ0JfTUVTU0FHRV9CRUdJTgBTSVRFX0lTX0ZST1pFTgBIUEVfSU5WQUxJRF9IRUFERVJfVE9LRU4ASU5WQUxJRF9UT0tFTgBGT1JCSURERU4ARU5IQU5DRV9ZT1VSX0NBTE0ASFBFX0lOVkFMSURfVVJMAEJMT0NLRURfQllfUEFSRU5UQUxfQ09OVFJPTABNS0NPTABBQ0wASFBFX0lOVEVSTkFMAFJFUVVFU1RfSEVBREVSX0ZJRUxEU19UT09fTEFSR0VfVU5PRkZJQ0lBTABIUEVfT0sAVU5MSU5LAFVOTE9DSwBQUkkAUkVUUllfV0lUSABIUEVfSU5WQUxJRF9DT05URU5UX0xFTkdUSABIUEVfVU5FWFBFQ1RFRF9DT05URU5UX0xFTkdUSABGTFVTSABQUk9QUEFUQ0gATS1TRUFSQ0gAVVJJX1RPT19MT05HAFBST0NFU1NJTkcATUlTQ0VMTEFORU9VU19QRVJTSVNURU5UX1dBUk5JTkcATUlTQ0VMTEFORU9VU19XQVJOSU5HAEhQRV9JTlZBTElEX1RSQU5TRkVSX0VOQ09ESU5HAEV4cGVjdGVkIENSTEYASFBFX0lOVkFMSURfQ0hVTktfU0laRQBNT1ZFAENPTlRJTlVFAEhQRV9DQl9TVEFUVVNfQ09NUExFVEUASFBFX0NCX0hFQURFUlNfQ09NUExFVEUASFBFX0NCX1ZFUlNJT05fQ09NUExFVEUASFBFX0NCX1VSTF9DT01QTEVURQBIUEVfQ0JfQ0hVTktfQ09NUExFVEUASFBFX0NCX0hFQURFUl9WQUxVRV9DT01QTEVURQBIUEVfQ0JfQ0hVTktfRVhURU5TSU9OX1ZBTFVFX0NPTVBMRVRFAEhQRV9DQl9DSFVOS19FWFRFTlNJT05fTkFNRV9DT01QTEVURQBIUEVfQ0JfTUVTU0FHRV9DT01QTEVURQBIUEVfQ0JfTUVUSE9EX0NPTVBMRVRFAEhQRV9DQl9IRUFERVJfRklFTERfQ09NUExFVEUAREVMRVRFAEhQRV9JTlZBTElEX0VPRl9TVEFURQBJTlZBTElEX1NTTF9DRVJUSUZJQ0FURQBQQVVTRQBOT19SRVNQT05TRQBVTlNVUFBPUlRFRF9NRURJQV9UWVBFAEdPTkUATk9UX0FDQ0VQVEFCTEUAU0VSVklDRV9VTkFWQUlMQUJMRQBSQU5HRV9OT1RfU0FUSVNGSUFCTEUAT1JJR0lOX0lTX1VOUkVBQ0hBQkxFAFJFU1BPTlNFX0lTX1NUQUxFAFBVUkdFAE1FUkdFAFJFUVVFU1RfSEVBREVSX0ZJRUxEU19UT09fTEFSR0UAUkVRVUVTVF9IRUFERVJfVE9PX0xBUkdFAFBBWUxPQURfVE9PX0xBUkdFAElOU1VGRklDSUVOVF9TVE9SQUdFAEhQRV9QQVVTRURfVVBHUkFERQBIUEVfUEFVU0VEX0gyX1VQR1JBREUAU09VUkNFAEFOTk9VTkNFAFRSQUNFAEhQRV9VTkVYUEVDVEVEX1NQQUNFAERFU0NSSUJFAFVOU1VCU0NSSUJFAFJFQ09SRABIUEVfSU5WQUxJRF9NRVRIT0QATk9UX0ZPVU5EAFBST1BGSU5EAFVOQklORABSRUJJTkQAVU5BVVRIT1JJWkVEAE1FVEhPRF9OT1RfQUxMT1dFRABIVFRQX1ZFUlNJT05fTk9UX1NVUFBPUlRFRABBTFJFQURZX1JFUE9SVEVEAEFDQ0VQVEVEAE5PVF9JTVBMRU1FTlRFRABMT09QX0RFVEVDVEVEAEhQRV9DUl9FWFBFQ1RFRABIUEVfTEZfRVhQRUNURUQAQ1JFQVRFRABJTV9VU0VEAEhQRV9QQVVTRUQAVElNRU9VVF9PQ0NVUkVEAFBBWU1FTlRfUkVRVUlSRUQAUFJFQ09ORElUSU9OX1JFUVVJUkVEAFBST1hZX0FVVEhFTlRJQ0FUSU9OX1JFUVVJUkVEAE5FVFdPUktfQVVUSEVOVElDQVRJT05fUkVRVUlSRUQATEVOR1RIX1JFUVVJUkVEAFNTTF9DRVJUSUZJQ0FURV9SRVFVSVJFRABVUEdSQURFX1JFUVVJUkVEAFBBR0VfRVhQSVJFRABQUkVDT05ESVRJT05fRkFJTEVEAEVYUEVDVEFUSU9OX0ZBSUxFRABSRVZBTElEQVRJT05fRkFJTEVEAFNTTF9IQU5EU0hBS0VfRkFJTEVEAExPQ0tFRABUUkFOU0ZPUk1BVElPTl9BUFBMSUVEAE5PVF9NT0RJRklFRABOT1RfRVhURU5ERUQAQkFORFdJRFRIX0xJTUlUX0VYQ0VFREVEAFNJVEVfSVNfT1ZFUkxPQURFRABIRUFEAEV4cGVjdGVkIEhUVFAvAABeEwAAJhMAADAQAADwFwAAnRMAABUSAAA5FwAA8BIAAAoQAAB1EgAArRIAAIITAABPFAAAfxAAAKAVAAAjFAAAiRIAAIsUAABNFQAA1BEAAM8UAAAQGAAAyRYAANwWAADBEQAA4BcAALsUAAB0FAAAfBUAAOUUAAAIFwAAHxAAAGUVAACjFAAAKBUAAAIVAACZFQAALBAAAIsZAABPDwAA1A4AAGoQAADOEAAAAhcAAIkOAABuEwAAHBMAAGYUAABWFwAAwRMAAM0TAABsEwAAaBcAAGYXAABfFwAAIhMAAM4PAABpDgAA2A4AAGMWAADLEwAAqg4AACgXAAAmFwAAxRMAAF0WAADoEQAAZxMAAGUTAADyFgAAcxMAAB0XAAD5FgAA8xEAAM8OAADOFQAADBIAALMRAAClEQAAYRAAADIXAAC7EwBB+TULAQEAQZA2C+ABAQECAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEAQf03CwEBAEGROAteAgMCAgICAgAAAgIAAgIAAgICAgICAgICAgAEAAAAAAACAgICAgICAgICAgICAgICAgICAgICAgICAgAAAAICAgICAgICAgICAgICAgICAgICAgICAgICAgICAAIAAgBB/TkLAQEAQZE6C14CAAICAgICAAACAgACAgACAgICAgICAgICAAMABAAAAAICAgICAgICAgICAgICAgICAgICAgICAgICAAAAAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAAgACAEHwOwsNbG9zZWVlcC1hbGl2ZQBBiTwLAQEAQaA8C+ABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEAAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEAQYk+CwEBAEGgPgvnAQEBAQEBAQEBAQEBAQIBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBY2h1bmtlZABBsMAAC18BAQABAQEBAQAAAQEAAQEAAQEBAQEBAQEBAQAAAAAAAAABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQAAAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAAEAAQBBkMIACyFlY3Rpb25lbnQtbGVuZ3Rob25yb3h5LWNvbm5lY3Rpb24AQcDCAAstcmFuc2Zlci1lbmNvZGluZ3BncmFkZQ0KDQoNClNNDQoNClRUUC9DRS9UU1AvAEH5wgALBQECAAEDAEGQwwAL4AEEAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQBB+cQACwUBAgABAwBBkMUAC+ABBAEBBQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEAAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEAQfnGAAsEAQAAAQBBkccAC98BAQEAAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQBB+sgACwQBAAACAEGQyQALXwMEAAAEBAQEBAQEBAQEBAUEBAQEBAQEBAQEBAQABAAGBwQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAAEAAQABAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQAAAAEAEH6ygALBAEAAAEAQZDLAAsBAQBBqssAC0ECAAAAAAAAAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMAAAAAAAADAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwBB+swACwQBAAABAEGQzQALAQEAQZrNAAsGAgAAAAACAEGxzQALOgMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAAAAAAAAAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMAQfDOAAuWAU5PVU5DRUVDS09VVE5FQ1RFVEVDUklCRUxVU0hFVEVBRFNFQVJDSFJHRUNUSVZJVFlMRU5EQVJWRU9USUZZUFRJT05TQ0hTRUFZU1RBVENIR0VPUkRJUkVDVE9SVFJDSFBBUkFNRVRFUlVSQ0VCU0NSSUJFQVJET1dOQUNFSU5ETktDS1VCU0NSSUJFSFRUUC9BRFRQLw==","base64")}())}catch(A){
/* istanbul ignore next */
// We could check if the error was caused by the simd option not
// being enabled, but the occurring of this other error
// * https://github.com/emscripten-core/emscripten/issues/11495
// got me to remove that check to avoid breaking Node 12.
t=await WebAssembly.compile(e||Re())}return await WebAssembly.instantiate(t,{env:{
/* eslint-disable camelcase */
wasm_on_url:(A,e,t)=>0,wasm_on_status:(e,t,r)=>{A(sA.ptr===e);const n=t-aA+oA.byteOffset;return sA.onStatus(new X(oA.buffer,n,r))||0},wasm_on_message_begin:e=>(A(sA.ptr===e),sA.onMessageBegin()||0),wasm_on_header_field:(e,t,r)=>{A(sA.ptr===e);const n=t-aA+oA.byteOffset;return sA.onHeaderField(new X(oA.buffer,n,r))||0},wasm_on_header_value:(e,t,r)=>{A(sA.ptr===e);const n=t-aA+oA.byteOffset;return sA.onHeaderValue(new X(oA.buffer,n,r))||0},wasm_on_headers_complete:(e,t,r,n)=>(A(sA.ptr===e),sA.onHeadersComplete(t,Boolean(r),Boolean(n))||0),wasm_on_body:(e,t,r)=>{A(sA.ptr===e);const n=t-aA+oA.byteOffset;return sA.onBody(new X(oA.buffer,n,r))||0},wasm_on_message_complete:e=>(A(sA.ptr===e),sA.onMessageComplete()||0)
/* eslint-enable camelcase */}})}();nA.catch();let sA=null,oA=null,iA=0,aA=null;class gA{constructor(e,t,{exports:r}){A(Number.isFinite(e[G])&&e[G]>0),this.llhttp=r,this.ptr=this.llhttp.llhttp_alloc(z.TYPE.RESPONSE),this.client=e,this.socket=t,this.timeout=null,this.timeoutValue=null,this.timeoutType=null,this.statusCode=null,this.statusText="",this.upgrade=!1,this.headers=[],this.headersSize=0,this.headersMaxSize=e[G],this.shouldKeepAlive=!1,this.paused=!1,this.resume=this.resume.bind(this),this.bytesRead=0,this.keepAlive="",this.contentLength="",this.connection="",this.maxResponseSize=e[O]}setTimeout(A,e){
// If the existing timer and the new timer are of different timer type
// (fast or native) or have different delay, we need to clear the existing
// timer and set a new one.
A!==this.timeoutValue||1&e^1&this.timeoutType?(
// If a timeout is already set, clear it with clearTimeout of the fast
// timer implementation, as it can clear fast and native timers.
this.timeout&&(r.clearTimeout(this.timeout),this.timeout=null),A&&(1&e?this.timeout=r.setFastTimeout(cA,A,new WeakRef(this)):(this.timeout=setTimeout(cA,A,new WeakRef(this)),this.timeout.unref())),this.timeoutValue=A):this.timeout&&this.timeout.refresh&&this.timeout.refresh(),this.timeoutType=e}resume(){!this.socket.destroyed&&this.paused&&(A(null!=this.ptr),A(null==sA),this.llhttp.llhttp_resume(this.ptr),A(5===this.timeoutType),this.timeout&&this.timeout.refresh&&this.timeout.refresh(),this.paused=!1,this.execute(this.socket.read()||K),// Flush parser.
this.readMore())}readMore(){for(;!this.paused&&this.ptr;){const A=this.socket.read();if(null===A)break;this.execute(A)}}execute(t){A(null!=this.ptr),A(null==sA),A(!this.paused);const{socket:r,llhttp:n}=this;t.length>iA&&(aA&&n.free(aA),iA=4096*Math.ceil(t.length/4096),aA=n.malloc(iA)),new Uint8Array(n.memory.buffer,aA,iA).set(t);
// Call `execute` on the wasm parser.
// We pass the `llhttp_parser` pointer address, the pointer address of buffer view data,
// and finally the length of bytes to parse.
// The return value is an error code or `constants.ERROR.OK`.
try{let A;try{oA=t,sA=this,A=n.llhttp_execute(this.ptr,aA,t.length)}catch(A){
/* istanbul ignore next: difficult to make a test case for */
throw A}finally{sA=null,oA=null}const e=n.llhttp_get_error_pos(this.ptr)-aA;if(A!==z.ERROR.OK){const n=t.subarray(e);if(A===z.ERROR.PAUSED_UPGRADE)this.onUpgrade(n);else{if(A!==z.ERROR.PAUSED)throw this.createError(A,n);this.paused=!0,r.unshift(n)}}}catch(A){e.destroy(r,A)}}finish(){A(null===sA),A(null!=this.ptr),A(!this.paused);const{llhttp:e}=this;let t;try{sA=this,t=e.llhttp_finish(this.ptr)}finally{sA=null}return t===z.ERROR.OK?null:t===z.ERROR.PAUSED||t===z.ERROR.PAUSED_UPGRADE?(this.paused=!0,null):this.createError(t,K)}createError(A,e){const{llhttp:t,contentLength:r,bytesRead:n}=this;if(r&&n!==parseInt(r,10))return new s;const o=t.llhttp_get_error_reason(this.ptr);let i="";if(o){const A=new Uint8Array(t.memory.buffer,o).indexOf(0);i="Response does not match the HTTP/1.1 protocol ("+Buffer.from(t.memory.buffer,o,A).toString()+")"}return new h(i,z.ERROR[A],e)}destroy(){A(null!=this.ptr),A(null==sA),this.llhttp.llhttp_free(this.ptr),this.ptr=null,this.timeout&&r.clearTimeout(this.timeout),this.timeout=null,this.timeoutValue=null,this.timeoutType=null,this.paused=!1}onStatus(A){this.statusText=A.toString()}onMessageBegin(){const{socket:A,client:t}=this;
/* istanbul ignore next: difficult to make a test case for */if(A.destroyed)return-1;if(0===t[w])return e.destroy(A,new c("bad response",e.getSocketInfo(A))),-1;const r=t[k][t[N]];if(!r)return-1;r.onResponseStarted()}onHeaderField(A){const e=this.headers.length;1&e?this.headers[e-1]=Buffer.concat([this.headers[e-1],A]):this.headers.push(A),this.trackHeader(A.length)}onHeaderValue(A){let t=this.headers.length;1&~t?this.headers[t-1]=Buffer.concat([this.headers[t-1],A]):(this.headers.push(A),t+=1);const r=this.headers[t-2];if(10===r.length){const t=e.bufferToLowerCasedHeaderName(r);"keep-alive"===t?this.keepAlive+=A.toString():"connection"===t&&(this.connection+=A.toString())}else 14===r.length&&"content-length"===e.bufferToLowerCasedHeaderName(r)&&(this.contentLength+=A.toString());this.trackHeader(A.length)}trackHeader(A){this.headersSize+=A,this.headersSize>=this.headersMaxSize&&e.destroy(this.socket,new g)}onUpgrade(t){const{upgrade:r,client:n,socket:s,headers:o,statusCode:i,statusText:a}=this;A(r),A(n[L]===s),A(!s.destroyed),A(!this.paused),A(!(1&o.length));const g=n[k][n[N]];A(g),A(g.upgrade||"CONNECT"===g.method),this.statusCode=null,this.statusText="",this.shouldKeepAlive=null,this.headers=[],this.headersSize=0,s.unshift(t),s[f].destroy(),s[f]=null,s[d]=null,s[M]=null,j(s),n[L]=null,n[Z]=null,// TODO (fix): This is hacky...
n[k][n[N]++]=null,n.emit("disconnect",n[l],[n],new Q("upgrade"));try{g.onUpgrade(i,o,s,a)}catch(A){e.errorRequest(n,g,A),e.destroy(s,A)}n[q]()}onHeadersComplete(t,r,n){const{client:s,socket:o,headers:i,statusText:a}=this;
/* istanbul ignore next: difficult to make a test case for */if(o.destroyed)return-1;if(0===s[w])return e.destroy(o,new c("bad response",e.getSocketInfo(o))),-1;const g=s[k][s[N]];
/* istanbul ignore next: difficult to make a test case for */if(!g)return-1;if(A(!this.upgrade),A(this.statusCode<200),100===t)return e.destroy(o,new c("bad response",e.getSocketInfo(o))),-1;
/* this can only happen if server is misbehaving */if(r&&!g.upgrade)return e.destroy(o,new c("bad upgrade",e.getSocketInfo(o))),-1;if(A(3===this.timeoutType),this.statusCode=t,this.shouldKeepAlive=n||
// Override llhttp value which does not allow keepAlive for HEAD.
"HEAD"===g.method&&!o[u]&&"keep-alive"===this.connection.toLowerCase(),this.statusCode>=200){const A=null!=g.bodyTimeout?g.bodyTimeout:s[H];this.setTimeout(A,5)}else this.timeout&&this.timeout.refresh&&this.timeout.refresh();if("CONNECT"===g.method)return A(1===s[w]),this.upgrade=!0,2;if(r)return A(1===s[w]),this.upgrade=!0,2;if(A(!(1&this.headers.length)),this.headers=[],this.headersSize=0,this.shouldKeepAlive&&s[U]){const A=this.keepAlive?e.parseKeepAliveTimeout(this.keepAlive):null;if(null!=A){const e=Math.min(A-s[v],s[Y]);e<=0?o[u]=!0:s[T]=e}else s[T]=s[b]}else
// Stop more requests from being dispatched.
o[u]=!0;const Q=!1===g.onHeaders(t,i,this.resume,a);return g.aborted?-1:"HEAD"===g.method||t<200?1:(o[p]&&(o[p]=!1,s[q]()),Q?z.ERROR.PAUSED:0)}onBody(t){const{client:r,socket:n,statusCode:s,maxResponseSize:o}=this;if(n.destroyed)return-1;const i=r[k][r[N]];return A(i),A(5===this.timeoutType),this.timeout&&this.timeout.refresh&&this.timeout.refresh(),A(s>=200),o>-1&&this.bytesRead+t.length>o?(e.destroy(n,new I),-1):(this.bytesRead+=t.length,!1===i.onData(t)?z.ERROR.PAUSED:void 0)}onMessageComplete(){const{client:t,socket:r,statusCode:n,upgrade:o,headers:i,contentLength:a,bytesRead:g,shouldKeepAlive:c}=this;if(r.destroyed&&(!n||c))return-1;if(o)return;A(n>=100),A(!(1&this.headers.length));const E=t[k][t[N]];return A(E),this.statusCode=null,this.statusText="",this.bytesRead=0,this.contentLength="",this.keepAlive="",this.connection="",this.headers=[],this.headersSize=0,n<200?void 0
/* istanbul ignore next: should be handled by llhttp? */:"HEAD"!==E.method&&a&&g!==parseInt(a,10)?(e.destroy(r,new s),-1):(E.onComplete(i),t[k][t[N]++]=null,r[eA]=!0,r[m]?(A(0===t[w]),
// Response completed before request.
e.destroy(r,new Q("reset")),z.ERROR.PAUSED):c?r[u]&&0===t[w]?(
// Destroy socket once all requests have completed.
// The request at the tail of the pipeline is the one
// that requested reset and no further requests should
// have been queued since then.
e.destroy(r,new Q("reset")),z.ERROR.PAUSED):void(null==t[U]||1===t[U]?
// We must wait a full event loop cycle to reuse this socket to make sure
// that non-spec compliant servers are not closing the connection even if they
// said they won't.
setImmediate(()=>t[q]()):t[q]()):(e.destroy(r,new Q("reset")),z.ERROR.PAUSED))}}function cA(t){const{socket:r,timeoutType:n,client:s,paused:o}=t.deref();
/* istanbul ignore else */3===n?(!r[m]||r.writableNeedDrain||s[w]>1)&&(A(!o,"cannot be paused while waiting for headers"),e.destroy(r,new a)):5===n?o||e.destroy(r,new B):8===n&&(A(0===s[w]&&s[T]),e.destroy(r,new Q("socket idle timeout")))}function QA(A){A[AA]&&(clearImmediate(A[AA]),A[AA]=null),A[$]=0}function EA(t,r,n,s,o,i,a,g){try{r?e.isBuffer(r)&&(A(i===r.byteLength,"buffer body must have content length"),o.cork(),o.write(`${a}content-length: ${i}\r\n\r\n`,"latin1"),o.write(r),o.uncork(),s.onBodySent(r),g||!1===s.reset||(o[u]=!0)):0===i?o.write(`${a}content-length: 0\r\n\r\n`,"latin1"):(A(null===i,"no body must not have content length"),o.write(`${a}\r\n`,"latin1")),s.onRequestSent(),n[q]()}catch(A){t(A)}}async function IA(e,t,r,n,s,o,i,a){A(0!==o||0===r[w],"iterator body cannot be pipelined");let g=null;function c(){if(g){const A=g;g=null,A()}}const Q=()=>new Promise((e,t)=>{A(null===g),s[M]?t(s[M]):g=e});s.on("close",c).on("drain",c);const E=new uA({abort:e,socket:s,request:n,contentLength:o,client:r,expectsPayload:a,header:i});try{
// It's up to the user to somehow abort the async iterable.
for await(const A of t){if(s[M])throw s[M];E.write(A)||await Q()}E.end()}catch(A){E.destroy(A)}finally{s.off("close",c).off("drain",c)}}class uA{constructor({abort:A,socket:e,request:t,contentLength:r,client:n,expectsPayload:s,header:o}){this.socket=e,this.request=t,this.contentLength=r,this.client=n,this.bytesWritten=0,this.expectsPayload=s,this.header=o,this.abort=A,e[m]=!0}write(A){const{socket:e,request:t,contentLength:r,client:s,bytesWritten:o,expectsPayload:i,header:a}=this;if(e[M])throw e[M];if(e.destroyed)return!1;const g=Buffer.byteLength(A);if(!g)return!0;
// We should defer writing chunks.
if(null!==r&&o+g>r){if(s[V])throw new n;process.emitWarning(new n)}e.cork(),0===o&&(i||!1===t.reset||(e[u]=!0),null===r?e.write(`${a}transfer-encoding: chunked\r\n`,"latin1"):e.write(`${a}content-length: ${r}\r\n\r\n`,"latin1")),null===r&&e.write(`\r\n${g.toString(16)}\r\n`,"latin1"),this.bytesWritten+=g;const c=e.write(A);return e.uncork(),t.onBodySent(A),c||e[f].timeout&&3===e[f].timeoutType&&e[f].timeout.refresh&&e[f].timeout.refresh(),c}end(){const{socket:A,contentLength:e,client:t,bytesWritten:r,expectsPayload:s,header:o,request:i}=this;if(i.onRequestSent(),A[m]=!1,A[M])throw A[M];if(!A.destroyed){if(0===r?s?
// https://tools.ietf.org/html/rfc7230#section-3.3.2
// A user agent SHOULD send a Content-Length in a request message when
// no Transfer-Encoding is sent and the request method defines a meaning
// for an enclosed payload body.
A.write(`${o}content-length: 0\r\n\r\n`,"latin1"):A.write(`${o}\r\n`,"latin1"):null===e&&A.write("\r\n0\r\n\r\n","latin1"),null!==e&&r!==e){if(t[V])throw new n;process.emitWarning(new n)}A[f].timeout&&3===A[f].timeoutType&&A[f].timeout.refresh&&A[f].timeout.refresh(),t[q]()}}destroy(e){const{socket:t,client:r,abort:n}=this;t[m]=!1,e&&(A(r[w]<=1,"pipeline should only contain this request"),n(e))}}return zA=async function(r,s){r[L]=s,rA||(rA=await nA,nA=null),s[R]=!1,s[m]=!1,s[u]=!1,s[p]=!1,s[$]=0,s[AA]=null,s[eA]=!1,s[f]=new gA(r,s,rA),_(s,"error",function(e){A("ERR_TLS_CERT_ALTNAME_INVALID"!==e.code);const t=this[f];
// On Mac OS, we get an ECONNRESET even if there is a full body to be forwarded
// to the user.
if("ECONNRESET"===e.code&&t.statusCode&&!t.shouldKeepAlive){const A=t.finish();return void(A&&(this[M]=A,this[d][P](A)))}this[M]=e,this[d][P](e)}),_(s,"readable",function(){const A=this[f];A&&A.readMore()}),_(s,"end",function(){const A=this[f];if(A.statusCode&&!A.shouldKeepAlive){const t=A.finish();return void(t&&e.destroy(this,t))}e.destroy(this,new c("other side closed",e.getSocketInfo(this)))}),_(s,"close",function(){const t=this[d],r=this[f];QA(this),r&&(this[M]||!r.statusCode||r.shouldKeepAlive||(this[M]=r.finish()||this[M]),this[f].destroy(),this[f]=null);const n=this[M]||new c("closed",e.getSocketInfo(this));// TODO (fix): This is hacky...
if(t[L]=null,t[Z]=null,t.destroyed){A(0===t[y]);
// Fail entire queue.
const r=t[k].splice(t[N]);for(let A=0;A<r.length;A++){const s=r[A];e.errorRequest(t,s,n)}}else if(t[w]>0&&"UND_ERR_INFO"!==n.code){
// Fail head of pipeline.
const A=t[k][t[N]];t[k][t[N]++]=null,e.errorRequest(t,A,n)}t[S]=t[N],A(0===t[w]),t.emit("disconnect",t[l],[t],n),t[q]()});let a=!1;return s.on("close",()=>{a=!0}),{version:"h1",defaultPipelining:1,write:(...s)=>function(r,s){const{method:a,path:g,host:c,upgrade:E,blocking:B,reset:h}=s;let{body:I,headers:C,contentLength:l}=s;
// https://tools.ietf.org/html/rfc7231#section-4.3.1
// https://tools.ietf.org/html/rfc7231#section-4.3.2
// https://tools.ietf.org/html/rfc7231#section-4.3.5
// Sending a payload body on a request that does not
// expect it can cause undefined behavior on some
// servers and corrupt connection state. Do not
// re-use the connection for further requests.
const d="PUT"===a||"POST"===a||"PATCH"===a||"QUERY"===a||"PROPFIND"===a||"PROPPATCH"===a;if(e.isFormDataLike(I)){tA||(tA=Ge().extractBody);const[A,e]=tA(I);null==s.contentType&&C.push("content-type",e),I=A.stream,l=A.length}else if(e.isBlobLike(I)&&null==s.contentType){const A=I.type;if(A){const t=`${A}`;if(!e.isValidHeaderValue(t))return e.errorRequest(r,s,new i("invalid content-type header")),!1;C.push("content-type",t)}}I&&"function"==typeof I.read&&
// Try to read EOF in order to get length.
I.read(0);const f=e.bodyLength(I);l=f??l,null===l&&(l=s.contentLength);0!==l||d||(
// https://tools.ietf.org/html/rfc7230#section-3.3.2
// A user agent SHOULD NOT send a Content-Length header field when
// the request message does not contain a payload body and the method
// semantics do not anticipate such a body.
l=null);
// https://github.com/nodejs/undici/issues/2046
// A user agent may send a Content-Length header with 0 value, this should be allowed.
if(
// https://www.rfc-editor.org/rfc/rfc7230#section-3.3.2
function(A){return"GET"!==A&&"HEAD"!==A&&"OPTIONS"!==A&&"TRACE"!==A&&"CONNECT"!==A}(a)&&l>0&&null!==s.contentLength&&s.contentLength!==l){if(r[V])return e.errorRequest(r,s,new n),!1;process.emitWarning(new n)}const y=r[L];QA(y);
/**
	   * @param {Error} [error]
	   */
const D=A=>{s.aborted||(s.completed?(s.upgrade||"CONNECT"===s.method)&&e.destroy(y,new Q("aborted")):(e.errorRequest(r,s,A||new o),e.destroy(I),e.destroy(y,new Q("aborted"))))};try{s.onConnect(D)}catch(A){e.errorRequest(r,s,A)}if(s.aborted)return!1;"HEAD"===a&&(
// https://github.com/mcollina/undici/issues/258
// Close after a HEAD request to interop with misbehaving servers
// that may send a body in the response.
y[u]=!0);(E||"CONNECT"===a)&&(
// On CONNECT or upgrade, block pipeline from dispatching further
// requests on this connection.
y[u]=!0);null!=h&&(y[u]=h);r[x]&&y[W]++>=r[x]&&(y[u]=!0);B&&(y[p]=!0);let k=`${a} ${g} HTTP/1.1\r\n`;k+="string"==typeof c?`host: ${c}\r\n`:r[F];E?k+=`connection: upgrade\r\nupgrade: ${E}\r\n`:r[U]&&!y[u]?k+="connection: keep-alive\r\n":k+="connection: close\r\n";if(Array.isArray(C))for(let A=0;A<C.length;A+=2){const e=C[A+0],t=C[A+1];if(Array.isArray(t))for(let A=0;A<t.length;A++)k+=`${e}: ${t[A]}\r\n`;else k+=`${e}: ${t}\r\n`}t.sendHeaders.hasSubscribers&&t.sendHeaders.publish({request:s,headers:k,socket:y})
/* istanbul ignore else: assertion */;I&&0!==f?e.isBuffer(I)?EA(D,I,r,s,y,l,k,d):e.isBlobLike(I)?"function"==typeof I.stream?IA(D,I.stream(),r,s,y,l,k,d):async function(e,t,r,s,o,i,a,g){A(i===t.size,"blob body must have content length");try{if(null!=i&&i!==t.size)throw new n;const A=Buffer.from(await t.arrayBuffer());o.cork(),o.write(`${a}content-length: ${i}\r\n\r\n`,"latin1"),o.write(A),o.uncork(),s.onBodySent(A),s.onRequestSent(),g||!1===s.reset||(o[u]=!0),r[q]()}catch(A){e(A)}}(D,I,r,s,y,l,k,d):e.isStream(I)?function(t,r,n,s,i,a,g,c){A(0!==a||0===n[w],"stream body cannot be pipelined");let Q=!1;const E=new uA({abort:t,socket:i,request:s,contentLength:a,client:n,expectsPayload:c,header:g}),B=function(A){if(!Q)try{!E.write(A)&&this.pause&&this.pause()}catch(A){e.destroy(this,A)}},h=function(){Q||r.resume&&r.resume()},I=function(){if(
// 'close' might be emitted *before* 'error' for
// broken streams. Wait a tick to avoid this case.
queueMicrotask(()=>{
// It's only safe to remove 'error' listener after
// 'close'.
r.removeListener("error",C)}),!Q){const A=new o;queueMicrotask(()=>C(A))}},C=function(t){if(!Q){if(Q=!0,A(i.destroyed||i[m]&&n[w]<=1),i.off("drain",h).off("error",C),r.removeListener("data",B).removeListener("end",C).removeListener("close",I),!t)try{E.end()}catch(A){t=A}E.destroy(t),!t||"UND_ERR_INFO"===t.code&&"reset"===t.message?e.destroy(r):e.destroy(r,t)}};r.on("data",B).on("end",C).on("error",C).on("close",I),r.resume&&r.resume();i.on("drain",h).on("error",C),r.errorEmitted??r.errored?setImmediate(()=>C(r.errored)):(r.endEmitted??r.readableEnded)&&setImmediate(()=>C(null));(r.closeEmitted??r.closed)&&setImmediate(I)}(D,I,r,s,y,l,k,d):e.isIterable(I)?IA(D,I,r,s,y,l,k,d):A(!1):EA(D,null,r,s,y,l,k,d);return!0}(r,...s),resume(){!
/**
	 * @param {import('./client.js')} client
	 */
function(A){const e=A[L];if(e&&!e.destroyed){if(0===A[D]?!e[R]&&e.unref&&(e.unref(),e[R]=!0):e[R]&&e.ref&&(e.ref(),e[R]=!1),0===A[w]&&A[y]>0&&e[eA]){if(0===e[$])return function(A,e){e[$]=1,
// Yield to the check phase (after poll) so unsolicited bytes / FIN / RST
// already pending on this idle keep-alive socket are processed before the
// next request is written (GHSA-35p6-xmwp-9g52).
// setTimeout(0) pays Node's ~1ms timer floor on every sequential reuse
// (#5493). setImmediate avoids that, but an *unref'd* Immediate lets poll
// block for ~500ms when the event loop is otherwise idle (#5600 / #5606).
// A ref'd Immediate both keeps the pending request alive and makes poll
// return immediately — the hybrid those issues asked for.
e[AA]=setImmediate(()=>{e[AA]=null,e[$]=2,A[L]!==e||e.destroyed||A[q]()})}(A,e),e[f].readMore(),void e.destroyed;if(1===e[$])return e[f].readMore(),void e.destroyed}if(0===A[w]&&(e[f].readMore(),e.destroyed))return;if(0===A[D])8!==e[f].timeoutType&&e[f].setTimeout(A[T],8);else if(A[w]>0&&e[f].statusCode<200&&3!==e[f].timeoutType){const t=A[k][A[N]],r=null!=t.headersTimeout?t.headersTimeout:A[J];e[f].setTimeout(r,3)}}}(r)},destroy(A,e){a?queueMicrotask(e):s.destroy(A).on("close",e)},get destroyed(){return s.destroyed},busy(A){if(s[m]||s[u]||s[p]||1===s[$])return!0;if(A){if(r[w]>0&&!A.idempotent)
// Non-idempotent request cannot be retried.
// Ensure that no other requests are inflight and
// could cause failure.
return!0;if(r[w]>0&&(A.upgrade||"CONNECT"===A.method))
// Don't dispatch an upgrade until all preceding requests have completed.
// A misbehaving server might upgrade the connection before all pipelined
// request has completed.
return!0;if(r[w]>0&&0!==e.bodyLength(A.body)&&(e.isStream(A.body)||e.isAsyncIterable(A.body)||e.isFormDataLike(A.body)))
// Request with stream or iterator body can error while other requests
// are inflight and indirectly error those as well.
// Ensure this doesn't happen by waiting for inflight
// to complete before dispatching.
// Request with stream or iterator body cannot be retried.
// Ensure that no other requests are inflight and
// could cause failure.
return!0}return!1}}},zA}function ve(){if(_A)return XA;_A=1;const A=E,{errorMonitor:e}=d,{pipeline:t}=I,r=CA(),{RequestContentLengthMismatchError:n,RequestAbortedError:s,SocketError:o,InformationalError:i}=hA(),{kUrl:a,kReset:g,kClient:c,kRunning:Q,kPending:B,kQueue:h,kPendingIdx:C,kRunningIdx:l,kError:u,kSocket:f,kStrictContentLength:p,kOnError:w,kMaxConcurrentStreams:y,kHTTP2Session:D,kResume:m,kSize:k,kHTTPContext:R}=BA(),b=Symbol("open streams");let F,S,N=!1;
// Experimental
try{S=require("node:http2")}catch{
// @ts-ignore
S={constants:{}}}const{constants:{HTTP2_HEADER_AUTHORITY:M,HTTP2_HEADER_METHOD:U,HTTP2_HEADER_PATH:L,HTTP2_HEADER_SCHEME:T,HTTP2_HEADER_CONTENT_LENGTH:G,HTTP2_HEADER_EXPECT:Y,HTTP2_HEADER_STATUS:v}}=S;function J(A){const e=[];for(const[t,r]of Object.entries(A))
// h2 may concat the header value by array
// e.g. Set-Cookie
if(Array.isArray(r))for(const A of r)
// we need to provide each header value of header name
// because the headers handler expect name-value pair
e.push(Buffer.from(t),Buffer.from(A));else e.push(Buffer.from(t),Buffer.from(r));return e}
/**
	 * @param {import('node:http2').IncomingHttpHeaders} headers
	 * @returns {Buffer[]}
	 */function H(A){const{[v]:e,...t}=A;return J(t)}function V(e){A("ERR_TLS_CERT_ALTNAME_INVALID"!==e.code),this[f][u]=e,this[c][w](e)}function x(A,e,t){if(0===t){const t=new i(`HTTP/2: "frameError" received - type ${A}, code ${e}`);this[f][u]=t,this[c][w](t)}}function W(){const A=new o("other side closed",r.getSocketInfo(this[f]));this.destroy(A),r.destroy(this[f],A)}
/**
	 * This is the root cause of #3011
	 * We need to handle GOAWAY frames properly, and trigger the session close
	 * along with the socket right away
	 */function O(e){
// We cannot recover, so best to close the session and the socket
const t=this[u]||new o(`HTTP/2: "GOAWAY" frame received with code ${e}`,r.getSocketInfo(this)),n=this[c];
// Fail head of pipeline.
if(n[f]=null,n[R]=null,null!=this[D]&&(this[D].destroy(t),this[D]=null),r.destroy(this[f],t),n[l]<n[h].length){const A=n[h][n[l]];n[h][n[l]++]=null,r.errorRequest(n,A,t),n[C]=n[l]}A(0===n[Q]),n.emit("disconnect",n[a],[n],t),n[m]()}
// https://www.rfc-editor.org/rfc/rfc7230#section-3.3.2
function P(e,t,n,s,o,i,a,c){try{null!=n&&r.isBuffer(n)&&(A(a===n.byteLength,"buffer body must have content length"),t.cork(),t.write(n),t.uncork(),t.end(),o.onBodySent(n)),c||(i[g]=!0),o.onRequestSent(),s[m]()}catch(A){e(A)}}async function q(e,t,r,n,s,o,i,a){A(0!==i||0===n[Q],"iterator body cannot be pipelined");let c=null;function E(){if(c){const A=c;c=null,A()}}const B=()=>new Promise((e,t)=>{A(null===c),o[u]?t(o[u]):c=e});t.on("close",E).on("drain",E);try{
// It's up to the user to somehow abort the async iterable.
for await(const A of r){if(o[u])throw o[u];const e=t.write(A);s.onBodySent(A),e||await B()}t.end(),s.onRequestSent(),a||(o[g]=!0),n[m]()}catch(A){e(A)}finally{t.off("close",E).off("drain",E)}}return XA=async function(E,I){E[f]=I,N||(N=!0,process.emitWarning("H2 support is experimental, expect them to change at any time.",{code:"UNDICI-H2"}));const d=S.connect(E[a],{createConnection:()=>I,peerMaxConcurrentStreams:E[y]});d[b]=0,d[c]=E,d[f]=I,r.addListener(d,"error",V),r.addListener(d,"frameError",x),r.addListener(d,"end",W),r.addListener(d,"goaway",O),r.addListener(d,"close",function(){const{[c]:e}=this,{[f]:t}=e,n=this[f][u]||this[u]||new o("closed",r.getSocketInfo(t));if(e[D]=null,e.destroyed){A(0===e[B]);
// Fail entire queue.
const t=e[h].splice(e[l]);for(let A=0;A<t.length;A++){const s=t[A];r.errorRequest(e,s,n)}}}),d.unref(),E[D]=d,I[D]=d,r.addListener(I,"error",function(e){A("ERR_TLS_CERT_ALTNAME_INVALID"!==e.code),this[u]=e,this[c][w](e)}),r.addListener(I,"end",function(){r.destroy(this,new o("other side closed",r.getSocketInfo(this)))}),r.addListener(I,"close",function(){const e=this[u]||new o("closed",r.getSocketInfo(this));E[f]=null,null!=this[D]&&this[D].destroy(e),E[C]=E[l],A(0===E[Q]),E.emit("disconnect",E[a],[E],e),E[m]()});let R=!1;return I.on("close",()=>{R=!0}),{version:"h2",defaultPipelining:1/0,write:(...o)=>function(o,c){const E=o[D],{method:B,path:I,host:u,upgrade:d,expectContinue:w,signal:y,headers:k}=c;let{body:R}=c;if(d)return r.errorRequest(o,c,new Error("Upgrade not supported for H2")),!1;const S={};for(let A=0;A<k.length;A+=2){const e=k[A+0],t=k[A+1];if(Array.isArray(t))for(let A=0;A<t.length;A++)S[e]?S[e]+=`,${t[A]}`:S[e]=t[A];else S[e]=t}
/** @type {import('node:http2').ClientHttp2Stream} */let N;const{hostname:V,port:x}=o[a];S[M]=u||`${V}${x?`:${x}`:""}`,S[U]=B;
/**
	   * @param {Error} [error]
	   */
const W=A=>{c.aborted||(c.completed?"CONNECT"===B&&null!=N&&r.destroy(N,A||new s):(A=A||new s,r.errorRequest(o,c,A),null!=N&&r.destroy(N,A),
// We do not destroy the socket as we can continue using the session
// the stream get's destroyed and the session remains to create new streams
r.destroy(R,A),o[h][o[l]++]=null,o[m]()))};try{
// We are already connected, streams are pending.
// We can call on connect, and wait for abort
c.onConnect(W)}catch(A){r.errorRequest(o,c,A)}if(c.aborted)return!1;if("CONNECT"===B){E.ref(),
// We disabled endStream to allow the user to write to the stream
N=E.request(S,{endStream:!1,signal:y});let A=!1;
/**
	     * @param {import('node:http2').IncomingHttpHeaders} headers
	     */const t=t=>{A=!0,N.off(e,r),c.onUpgradeResponse(Number(t[v]),t,H)},r=e=>{A=!0,N.off("response",t),c.onUpgradeError(e)},n=()=>{try{c.onUpgrade(null,null,N)}catch(A){return N.off("response",t),void W(A)}c.aborted||(N.off("error",W),N.once(e,r),o[h][o[l]++]=null)};
/**
	     * @param {Error} error
	     */return N.once("response",t),N.once("error",W),++E[b],n(),N.once("close",()=>{!A&&c.completed&&(N.off("response",t),N.off(e,r),c.onUpgradeError(new i(`HTTP/2: "stream error" received - code ${N.rstCode}`))),E[b]-=1,0===E[b]&&E.unref()}),!0}
// https://tools.ietf.org/html/rfc7540#section-8.3
// :path and :scheme headers must be omitted when sending CONNECT
S[L]=I,S[T]="https";
// https://tools.ietf.org/html/rfc7231#section-4.3.1
// https://tools.ietf.org/html/rfc7231#section-4.3.2
// https://tools.ietf.org/html/rfc7231#section-4.3.5
// Sending a payload body on a request that does not
// expect it can cause undefined behavior on some
// servers and corrupt connection state. Do not
// re-use the connection for further requests.
const O="PUT"===B||"POST"===B||"PATCH"===B;R&&"function"==typeof R.read&&
// Try to read EOF in order to get length.
R.read(0);let Z=r.bodyLength(R);if(r.isFormDataLike(R)){F??=Ge().extractBody;const[A,e]=F(R);S["content-type"]=e,R=A.stream,Z=A.length}null==Z&&(Z=c.contentLength);0!==Z&&O||(
// https://tools.ietf.org/html/rfc7230#section-3.3.2
// A user agent SHOULD NOT send a Content-Length header field when
// the request message does not contain a payload body and the method
// semantics do not anticipate such a body.
Z=null);
// https://github.com/nodejs/undici/issues/2046
// A user agent may send a Content-Length header with 0 value, this should be allowed.
if(function(A){return"GET"!==A&&"HEAD"!==A&&"OPTIONS"!==A&&"TRACE"!==A&&"CONNECT"!==A}(B)&&Z>0&&null!=c.contentLength&&c.contentLength!==Z){if(o[p])return r.errorRequest(o,c,new n),!1;process.emitWarning(new n)}null!=Z&&(A(R,"no body must not have content length"),S[G]=`${Z}`);E.ref();const z="GET"===B||"HEAD"===B||null===R;w?(S[Y]="100-continue",N=E.request(S,{endStream:z,signal:y}),N.once("continue",K)):(N=E.request(S,{endStream:z,signal:y}),K());
// Increment counter as we have new streams open
// stream.on('aborted', () => {
//   // TODO(HTTP/2): Support aborted
// })
// stream.on('timeout', () => {
//   // TODO(HTTP/2): Support timeout
// })
// stream.on('push', headers => {
//   // TODO(HTTP/2): Support push
// })
// stream.on('trailers', headers => {
//   // TODO(HTTP/2): Support trailers
// })
return++E[b],N.once("response",A=>{const{[v]:e,...t}=A;
// Due to the stream nature, it is possible we face a race condition
// where the stream has been assigned, but the request has been aborted
// the request remains in-flight and headers hasn't been received yet
// for those scenarios, best effort is to destroy the stream immediately
// as there's no value to keep it open.
if(c.onResponseStarted(),c.aborted){const A=new s;return r.errorRequest(o,c,A),void r.destroy(N,A)}!1===c.onHeaders(Number(e),J(t),N.resume.bind(N),"")&&N.pause(),N.on("data",A=>{!1===c.onData(A)&&N.pause()})}),N.once("end",()=>{
// When state is null, it means we haven't consumed body and the stream still do not have
// a state.
// Present specially when using pipeline or stream
(null==N.state?.state||N.state.state<6)&&c.onComplete([]),0===E[b]&&
// Stream is closed or half-closed-remote (6), decrement counter and cleanup
// It does not have sense to continue working with the stream as we do not
// have yet RST_STREAM support on client-side
E.unref(),W(new i("HTTP/2: stream half-closed (remote)")),o[h][o[l]++]=null,o[C]=o[l],o[m]()}),N.once("close",()=>{E[b]-=1,0===E[b]&&E.unref()}),N.once("error",function(A){W(A)}),N.once("frameError",(A,e)=>{W(new i(`HTTP/2: "frameError" received - type ${A}, code ${e}`))}),!0;function K(){
/* istanbul ignore else: assertion */
R&&0!==Z?r.isBuffer(R)?P(W,N,R,o,c,o[f],Z,O):r.isBlobLike(R)?"function"==typeof R.stream?q(W,N,R.stream(),o,c,o[f],Z,O):async function(e,t,r,s,o,i,a,c){A(a===r.size,"blob body must have content length");try{if(null!=a&&a!==r.size)throw new n;const A=Buffer.from(await r.arrayBuffer());t.cork(),t.write(A),t.uncork(),t.end(),o.onBodySent(A),o.onRequestSent(),c||(i[g]=!0),s[m]()}catch(A){e(A)}}(W,N,R,o,c,o[f],Z,O):r.isStream(R)?function(e,n,s,o,i,a,c,E){A(0!==E||0===a[Q],"stream body cannot be pipelined");
// For HTTP/2, is enough to pipe the stream
const B=t(i,o,A=>{A?(r.destroy(B,A),e(A)):(r.removeAllListeners(B),c.onRequestSent(),s||(n[g]=!0),a[m]())});function h(A){c.onBodySent(A)}r.addListener(B,"data",h)}(W,o[f],O,N,R,o,c,Z):r.isIterable(R)?q(W,N,R,o,c,o[f],Z,O):A(!1):P(W,N,null,o,c,o[f],Z,O)}}(E,...o),resume(){!function(A){const e=A[f];!1===e?.destroyed&&(0===A[k]&&0===A[y]?(e.unref(),A[D].unref()):(e.ref(),A[D].ref()))}(E)},destroy(A,e){R?queueMicrotask(e):
// Destroying the socket will trigger the session close
I.destroy(A).on("close",e)},get destroyed(){return I.destroyed},busy:()=>!1}},XA}function Je(){if($A)return jA;$A=1;const A=CA(),{kBodyUsed:e}=BA(),t=E,{InvalidArgumentError:r}=hA(),n=d,s=[300,301,302,303,307,308],o=Symbol("body");class i{constructor(A){this[o]=A,this[e]=!1}async*[Symbol.asyncIterator](){t(!this[e],"disturbed"),this[e]=!0,yield*this[o]}}
// https://tools.ietf.org/html/rfc7231#section-6.4.4
function a(e,t,r){if(4===e.length)return"host"===A.headerNameToString(e);if(t&&A.headerNameToString(e).startsWith("content-"))return!0;if(r&&(13===e.length||6===e.length||19===e.length)){const t=A.headerNameToString(e);return"authorization"===t||"cookie"===t||"proxy-authorization"===t}return!1}
// https://tools.ietf.org/html/rfc7231#section-6.4
return jA=class{constructor(s,o,a,g){if(null!=o&&(!Number.isInteger(o)||o<0))throw new r("maxRedirections must be a positive number");A.validateHandler(g,a.method,a.upgrade),this.dispatch=s,this.location=null,this.abort=null,this.opts={...a,maxRedirections:0},// opts must be a copy
this.maxRedirections=o,this.handler=g,this.history=[],this.redirectionLimitReached=!1,A.isStream(this.opts.body)?(
// TODO (fix): Provide some way for the user to cache the file to e.g. /tmp
// so that it can be dispatched again?
// TODO (fix): Do we need 100-expect support to provide a way to do this properly?
0===A.bodyLength(this.opts.body)&&this.opts.body.on("data",function(){t(!1)}),"boolean"!=typeof this.opts.body.readableDidRead&&(this.opts.body[e]=!1,n.prototype.on.call(this.opts.body,"data",function(){this[e]=!0}))):(this.opts.body&&"function"==typeof this.opts.body.pipeTo||this.opts.body&&"string"!=typeof this.opts.body&&!ArrayBuffer.isView(this.opts.body)&&A.isIterable(this.opts.body))&&(
// TODO (fix): We can't access ReadableStream internal state
// to determine whether or not it has been disturbed. This is just
// a workaround.
this.opts.body=new i(this.opts.body))}onConnect(A){this.abort=A,this.handler.onConnect(A,{history:this.history})}onUpgrade(A,e,t){this.handler.onUpgrade(A,e,t)}onError(A){this.handler.onError(A)}onHeaders(e,r,n,o){if(this.location=this.history.length>=this.maxRedirections||A.isDisturbed(this.opts.body)?null:function(e,t){if(-1===s.indexOf(e))return null;for(let e=0;e<t.length;e+=2)if(8===t[e].length&&"location"===A.headerNameToString(t[e]))return t[e+1]}(e,r),this.opts.throwOnMaxRedirect&&this.history.length>=this.maxRedirections)return this.request&&this.request.abort(new Error("max redirects")),this.redirectionLimitReached=!0,void this.abort(new Error("max redirects"));if(this.opts.origin&&this.history.push(new URL(this.opts.path,this.opts.origin)),!this.location)return this.handler.onHeaders(e,r,n,o);const{origin:i,pathname:g,search:c}=A.parseURL(new URL(this.location,this.opts.origin&&new URL(this.opts.path,this.opts.origin))),Q=c?`${g}${c}`:g;
// Remove headers referring to the original URL.
// By default it is Host only, unless it's a 303 (see below), which removes also all Content-* headers.
// https://tools.ietf.org/html/rfc7231#section-6.4
this.opts.headers=function(A,e,r){const n=[];if(Array.isArray(A))for(let t=0;t<A.length;t+=2)a(A[t],e,r)||n.push(A[t],A[t+1]);else if(A&&"object"==typeof A)for(const t of Object.keys(A))a(t,e,r)||n.push(t,A[t]);else t(null==A,"headers must be an object or an array");return n}(this.opts.headers,303===e,this.opts.origin!==i),this.opts.path=Q,this.opts.origin=i,this.opts.maxRedirections=0,this.opts.query=null,
// https://tools.ietf.org/html/rfc7231#section-6.4.4
// In case of HTTP 303, always replace method to be either HEAD or GET
303===e&&"HEAD"!==this.opts.method&&(this.opts.method="GET",this.opts.body=null)}onData(A){if(!this.location)return this.handler.onData(A)}onComplete(A){this.location?(
/*
	        https://tools.ietf.org/html/rfc7231#section-6.4

	        TLDR: undici always ignores 3xx response trailers as they are not expected in case of redirections
	        and neither are useful if present.

	        See comment on onData method above for more detailed information.
	      */
this.location=null,this.abort=null,this.dispatch(this.opts,this)):this.handler.onComplete(A)}onBodySent(A){this.handler.onBodySent&&this.handler.onBodySent(A)}},jA}function He(){if(ee)return Ae;ee=1;const A=Je();return Ae=function({maxRedirections:e}){return t=>function(r,n){const{maxRedirections:s=e}=r;if(!s)return t(r,n);const o=new A(t,s,r,n);// Stop sub dispatcher from also redirecting.
return r={...r,maxRedirections:0},t(r,o)}},Ae}function Ve(){if(re)return te;re=1;const A=E,e=B,t=h,r=CA(),{channels:n}=lA(),s=function(){if(rA)return tA;rA=1;const{InvalidArgumentError:A,NotSupportedError:e}=hA(),t=E,{isValidHTTPToken:r,isValidHeaderValue:n,isStream:s,destroy:o,isBuffer:i,isFormDataLike:a,isIterable:g,isBlobLike:c,buildURL:Q,validateHandler:B,getServerName:h,normalizedMethodRecords:I}=CA(),{channels:C}=lA(),{headerNameLowerCasedRecord:l}=IA(),u=/[^\u0021-\u00ff]/,d=Symbol("handler");function f(t,s,o){if(o&&"object"==typeof o&&!Array.isArray(o))throw new A(`invalid ${s} header`);if(void 0===o)return;let i=l[s];if(void 0===i&&(i=s.toLowerCase(),void 0===l[i]&&!r(i)))throw new A("invalid header key");if(Array.isArray(o)){const e=[];for(let t=0;t<o.length;t++)if("string"==typeof o[t]){if(!n(o[t]))throw new A(`invalid ${s} header`);e.push(o[t])}else if(null===o[t])e.push("");else{if("object"==typeof o[t])throw new A(`invalid ${s} header`);{
// Coerce primitives (and reject unsafe coercions such as functions
// with a crafted toString/Symbol.toPrimitive).
const r=`${o[t]}`;if(!n(r))throw new A(`invalid ${s} header`);e.push(r)}}o=e}else if("string"==typeof o){if(!n(o))throw new A(`invalid ${s} header`)}else if(null===o)o="";else if(!n(
// Coerce primitives (and reject unsafe coercions such as functions
// with a crafted toString/Symbol.toPrimitive).
o=`${o}`))throw new A(`invalid ${s} header`);if("host"===i){if(null!==t.host)throw new A("duplicate host header");if("string"!=typeof o)throw new A("invalid host header");
// Consumed by Client
t.host=o}else if("content-length"===i){if(null!==t.contentLength)throw new A("duplicate content-length header");if(t.contentLength=parseInt(o,10),!Number.isFinite(t.contentLength))throw new A("invalid content-length header")}else if(null===t.contentType&&"content-type"===i)t.contentType=o,t.headers.push(s,o);else{if("transfer-encoding"===i||"keep-alive"===i||"upgrade"===i)throw new A(`invalid ${i} header`);if("connection"===i){const e="string"==typeof o?o.toLowerCase():null;if("close"!==e&&"keep-alive"!==e)throw new A("invalid connection header");"close"===e&&(t.reset=!0)}else{if("expect"===i)throw new e("expect header not supported");t.headers.push(s,o)}}}return tA=class{constructor(e,{path:t,method:E,body:l,headers:p,query:w,idempotent:y,blocking:D,upgrade:m,headersTimeout:k,bodyTimeout:R,reset:b,throwOnError:F,expectContinue:S,servername:N},M){if("string"!=typeof t)throw new A("path must be a string");if("/"!==t[0]&&!t.startsWith("http://")&&!t.startsWith("https://")&&"CONNECT"!==E)throw new A("path must be an absolute URL or start with a slash");if(u.test(t))throw new A("invalid request path");if("string"!=typeof E)throw new A("method must be a string");if(void 0===I[E]&&!r(E))throw new A("invalid request method");if(m&&"string"!=typeof m)throw new A("upgrade must be a string");if(m&&!n(m))throw new A("invalid upgrade header");if(null!=k&&(!Number.isFinite(k)||k<0))throw new A("invalid headersTimeout");if(null!=R&&(!Number.isFinite(R)||R<0))throw new A("invalid bodyTimeout");if(null!=b&&"boolean"!=typeof b)throw new A("invalid reset");if(null!=S&&"boolean"!=typeof S)throw new A("invalid expectContinue");if(this.headersTimeout=k,this.bodyTimeout=R,this.throwOnError=!0===F,this.method=E,this.abort=null,null==l)this.body=null;else if(s(l)){this.body=l;const A=this.body._readableState;A&&A.autoDestroy||(this.endHandler=function(){o(this)},this.body.on("end",this.endHandler)),this.errorHandler=A=>{this.abort?this.abort(A):this.error=A},this.body.on("error",this.errorHandler)}else if(i(l))this.body=l.byteLength?l:null;else if(ArrayBuffer.isView(l))this.body=l.buffer.byteLength?Buffer.from(l.buffer,l.byteOffset,l.byteLength):null;else if(l instanceof ArrayBuffer)this.body=l.byteLength?Buffer.from(l):null;else if("string"==typeof l)this.body=l.length?Buffer.from(l):null;else{if(!(a(l)||g(l)||c(l)))throw new A("body must be a string, a Buffer, a Readable stream, an iterable, or an async iterable");this.body=l}if(this.completed=!1,this.aborted=!1,this.upgrade=m||null,this.path=w?Q(t,w):t,this.origin=e,this.idempotent=y??("HEAD"===E||"GET"===E),this.blocking=D??!1,this.reset=b??null,this.host=null,this.contentLength=null,this.contentType=null,this.headers=[],
// Only for H2
this.expectContinue=null!=S&&S,Array.isArray(p)){if(p.length%2!=0)throw new A("headers array must be even");for(let A=0;A<p.length;A+=2)f(this,p[A],p[A+1])}else if(p&&"object"==typeof p)if(p[Symbol.iterator])for(const e of p){if(!Array.isArray(e)||2!==e.length)throw new A("headers must be in key-value pair format");f(this,e[0],e[1])}else{const A=Object.keys(p);for(let e=0;e<A.length;++e)f(this,A[e],p[A[e]])}else if(null!=p)throw new A("headers must be an object or an array");B(M,E,m),this.servername=N||h(this.host),this[d]=M,C.create.hasSubscribers&&C.create.publish({request:this})}onBodySent(A){if(this[d].onBodySent)try{return this[d].onBodySent(A)}catch(A){this.abort(A)}}onRequestSent(){if(C.bodySent.hasSubscribers&&C.bodySent.publish({request:this}),this[d].onRequestSent)try{return this[d].onRequestSent()}catch(A){this.abort(A)}}onConnect(A){if(t(!this.aborted),t(!this.completed),!this.error)return this.abort=A,this[d].onConnect(A);A(this.error)}onResponseStarted(){return this[d].onResponseStarted?.()}onHeaders(A,e,r,n){t(!this.aborted),t(!this.completed),C.headers.hasSubscribers&&C.headers.publish({request:this,response:{statusCode:A,headers:e,statusText:n}});try{return this[d].onHeaders(A,e,r,n)}catch(A){this.abort(A)}}onData(A){t(!this.aborted),t(!this.completed);try{return this[d].onData(A)}catch(A){return this.abort(A),!1}}
/**
	   * @param {number|null} statusCode
	   * @param {Buffer[]|null} headers
	   * @param {import('node:stream').Duplex} socket
	   * @param {string} [statusText]
	   */onUpgrade(A,e,r,n=""){this.onFinally(),t(!this.aborted),t(!this.completed),null!==A&&this.#o(A,e,n);const s=this[d].onUpgrade(A,e,r);return this.aborted||(this.completed=!0,null!==A&&this.#i()),s}
/**
	   * @param {number} statusCode
	   * @param {import('node:http2').IncomingHttpHeaders} headers
	   * @param {(headers: import('node:http2').IncomingHttpHeaders) => Buffer[]} parseHeaders
	   * @param {string} [statusText]
	   */onUpgradeResponse(A,e,r,n=""){t(!this.aborted),t(this.completed),C.headers.hasSubscribers&&this.#o(A,r(e),n),this.#i()}
/**
	   * @param {Error} error
	   */onUpgradeError(A){t(!this.aborted),t(this.completed),C.error.hasSubscribers&&C.error.publish({request:this,error:A})}
/**
	   * @param {number} statusCode
	   * @param {Buffer[]} headers
	   * @param {string} statusText
	   */#o(A,e,t){C.headers.hasSubscribers&&C.headers.publish({request:this,response:{statusCode:A,headers:e,statusText:t}})}#i(){C.trailers.hasSubscribers&&C.trailers.publish({request:this,trailers:[]})}onComplete(A){this.onFinally(),t(!this.aborted),this.completed=!0,C.trailers.hasSubscribers&&C.trailers.publish({request:this,trailers:A});try{return this[d].onComplete(A)}catch(A){
// TODO (fix): This might be a bad idea?
this.onError(A)}}onError(A){if(this.onFinally(),C.error.hasSubscribers&&C.error.publish({request:this,error:A}),!this.aborted)return this.aborted=!0,this[d].onError(A)}onFinally(){this.errorHandler&&(this.body.off("error",this.errorHandler),this.errorHandler=null),this.endHandler&&(this.body.off("end",this.endHandler),this.endHandler=null)}addHeader(A,e){return f(this,A,e),this}},tA}(),o=dA(),{InvalidArgumentError:i,InformationalError:a,ClientDestroyedError:g}=hA(),c=pA(),{kUrl:Q,kServerName:I,kClient:C,kBusy:l,kConnect:u,kResuming:d,kRunning:f,kPending:p,kSize:w,kQueue:y,kConnected:D,kConnecting:m,kNeedDrain:k,kKeepAliveDefaultTimeout:R,kHostHeader:b,kPendingIdx:F,kRunningIdx:S,kError:N,kPipelining:M,kKeepAliveTimeoutValue:U,kMaxHeadersSize:L,kKeepAliveMaxTimeout:T,kKeepAliveTimeoutThreshold:G,kHeadersTimeout:Y,kBodyTimeout:v,kStrictContentLength:J,kConnector:H,kMaxRedirections:V,kMaxRequests:x,kCounter:W,kClose:O,kDestroy:P,kDispatch:q,kInterceptors:Z,kLocalAddress:z,kMaxResponseSize:K,kOnError:X,kHTTPContext:_,kMaxConcurrentStreams:j,kResume:$}=BA(),AA=Ye(),eA=ve();let nA=!1;const sA=Symbol("kClosedResolve"),oA=()=>{};function iA(A){return A[M]??A[_]?.defaultPipelining??1}
/**
	 * @type {import('../../types/client.js').default}
	 */const aA=He();function gA(e,t){if(0===e[f]&&"UND_ERR_INFO"!==t.code&&"UND_ERR_SOCKET"!==t.code){
// Error is not caused by running request and not a recoverable
// socket error.
A(e[F]===e[S]);const n=e[y].splice(e[S]);for(let A=0;A<n.length;A++){const s=n[A];r.errorRequest(e,s,t)}A(0===e[w])}}
/**
	 * @param {Client} client
	 * @returns
	 */async function cA(t){A(!t[m]),A(!t[_]);let{host:s,hostname:o,protocol:i,port:a}=t[Q];
// Resolve ipv6
if("["===o[0]){const t=o.indexOf("]");A(-1!==t);const r=o.substring(1,t);A(e.isIP(r)),o=r}t[m]=!0,n.beforeConnect.hasSubscribers&&n.beforeConnect.publish({connectParams:{host:s,hostname:o,protocol:i,port:a,version:t[_]?.version,servername:t[I],localAddress:t[z]},connector:t[H]});try{const e=await new Promise((A,e)=>{t[H]({host:s,hostname:o,protocol:i,port:a,servername:t[I],localAddress:t[z]},(t,r)=>{t?e(t):A(r)})});if(t.destroyed)return void r.destroy(e.on("error",oA),new g);A(e);try{t[_]="h2"===e.alpnProtocol?await eA(t,e):await AA(t,e)}catch(A){throw e.destroy().on("error",oA),A}t[m]=!1,e[W]=0,e[x]=t[x],e[C]=t,e[N]=null,n.connected.hasSubscribers&&n.connected.publish({connectParams:{host:s,hostname:o,protocol:i,port:a,version:t[_]?.version,servername:t[I],localAddress:t[z]},connector:t[H],socket:e}),t.emit("connect",t[Q],[t])}catch(e){if(t.destroyed)return;if(t[m]=!1,n.connectError.hasSubscribers&&n.connectError.publish({connectParams:{host:s,hostname:o,protocol:i,port:a,version:t[_]?.version,servername:t[I],localAddress:t[z]},connector:t[H],error:e}),"ERR_TLS_CERT_ALTNAME_INVALID"===e.code)for(A(0===t[f]);t[p]>0&&t[y][t[F]].servername===t[I];){const A=t[y][t[F]++];r.errorRequest(t,A,e)}else gA(t,e);t.emit("connectionError",t[Q],[t],e)}t[$]()}function QA(A){A[k]=0,A.emit("drain",A[Q],[A])}function EA(e,t){2!==e[d]&&(e[d]=2,function(e,t){for(;;){if(e.destroyed)return void A(0===e[p]);if(e[sA]&&!e[w])return e[sA](),void(e[sA]=null);if(e[_]&&e[_].resume(),e[l])e[k]=2;else if(2===e[k]){t?(e[k]=1,queueMicrotask(()=>QA(e))):QA(e);continue}if(0===e[p])return;if(e[f]>=(iA(e)||1))return;const r=e[y][e[F]];if("https:"===e[Q].protocol&&e[I]!==r.servername){if(e[f]>0)return;e[I]=r.servername,e[_]?.destroy(new a("servername changed"),()=>{e[_]=null,EA(e)})}if(e[m])return;if(!e[_])return void cA(e);if(e[_].destroyed)return;if(e[_].busy(r))return;!r.aborted&&e[_].write(r)?e[F]++:e[y].splice(e[F],1)}}(e,t),e[d]=0,e[S]>256&&(e[y].splice(0,e[S]),e[F]-=e[S],e[S]=0))}return te=class extends o{
/**
	   *
	   * @param {string|URL} url
	   * @param {import('../../types/client.js').Client.Options} options
	   */
constructor(A,{interceptors:n,maxHeaderSize:s,headersTimeout:o,socketTimeout:a,requestTimeout:g,connectTimeout:E,bodyTimeout:B,idleTimeout:h,keepAlive:C,keepAliveTimeout:l,maxKeepAliveTimeout:u,keepAliveMaxTimeout:f,keepAliveTimeoutThreshold:p,socketPath:w,pipelining:D,tls:m,strictContentLength:N,maxCachedSessions:W,maxRedirections:O,connect:P,maxRequestsPerClient:q,localAddress:AA,maxResponseSize:eA,autoSelectFamily:tA,autoSelectFamilyAttemptTimeout:rA,maxConcurrentStreams:
// h2
oA,allowH2:iA,webSocket:cA}={}){if(super({webSocket:cA}),void 0!==C)throw new i("unsupported keepAlive, use pipelining=0 instead");if(void 0!==a)throw new i("unsupported socketTimeout, use headersTimeout & bodyTimeout instead");if(void 0!==g)throw new i("unsupported requestTimeout, use headersTimeout & bodyTimeout instead");if(void 0!==h)throw new i("unsupported idleTimeout, use keepAliveTimeout instead");if(void 0!==u)throw new i("unsupported maxKeepAliveTimeout, use keepAliveMaxTimeout instead");if(null!=s&&!Number.isFinite(s))throw new i("invalid maxHeaderSize");if(null!=w&&"string"!=typeof w)throw new i("invalid socketPath");if(null!=E&&(!Number.isFinite(E)||E<0))throw new i("invalid connectTimeout");if(null!=l&&(!Number.isFinite(l)||l<=0))throw new i("invalid keepAliveTimeout");if(null!=f&&(!Number.isFinite(f)||f<=0))throw new i("invalid keepAliveMaxTimeout");if(null!=p&&!Number.isFinite(p))throw new i("invalid keepAliveTimeoutThreshold");if(null!=o&&(!Number.isInteger(o)||o<0))throw new i("headersTimeout must be a positive integer or zero");if(null!=B&&(!Number.isInteger(B)||B<0))throw new i("bodyTimeout must be a positive integer or zero");if(null!=P&&"function"!=typeof P&&"object"!=typeof P)throw new i("connect must be a function or an object");if(null!=O&&(!Number.isInteger(O)||O<0))throw new i("maxRedirections must be a positive number");if(null!=q&&(!Number.isInteger(q)||q<0))throw new i("maxRequestsPerClient must be a positive number");if(null!=AA&&("string"!=typeof AA||0===e.isIP(AA)))throw new i("localAddress must be valid string IP address");if(null!=eA&&(!Number.isInteger(eA)||eA<-1))throw new i("maxResponseSize must be a positive number");if(null!=rA&&(!Number.isInteger(rA)||rA<-1))throw new i("autoSelectFamilyAttemptTimeout must be a positive number");
// h2
if(null!=iA&&"boolean"!=typeof iA)throw new i("allowH2 must be a valid boolean value");if(null!=oA&&("number"!=typeof oA||oA<1))throw new i("maxConcurrentStreams must be a positive integer, greater than 0");"function"!=typeof P&&(P=c({...m,maxCachedSessions:W,allowH2:iA,socketPath:w,timeout:E,...tA?{autoSelectFamily:tA,autoSelectFamilyAttemptTimeout:rA}:void 0,...P})),n?.Client&&Array.isArray(n.Client)?(this[Z]=n.Client,nA||(nA=!0,process.emitWarning("Client.Options#interceptor is deprecated. Use Dispatcher#compose instead.",{code:"UNDICI-CLIENT-INTERCEPTOR-DEPRECATED"}))):this[Z]=[aA({maxRedirections:O})],this[Q]=r.parseOrigin(A),this[H]=P,this[M]=null!=D?D:1,this[L]=s||t.maxHeaderSize,this[R]=l??4e3,this[T]=f??6e5,this[G]=p??2e3,this[U]=this[R],this[I]=null,this[z]=null!=AA?AA:null,this[d]=0,// 0, idle, 1, scheduled, 2 resuming
this[k]=0,// 0, idle, 1, scheduled, 2 resuming
this[b]=`host: ${this[Q].hostname}${this[Q].port?`:${this[Q].port}`:""}\r\n`,this[v]=null!=B?B:3e5,this[Y]=null!=o?o:3e5,this[J]=N??!0,this[V]=O,this[x]=q,this[sA]=null,this[K]=eA>-1?eA:-1,this[j]=null!=oA?oA:100,// Max peerConcurrentStreams for a Node h2 server
this[_]=null,
// kQueue is built up of 3 sections separated by
// the kRunningIdx and kPendingIdx indices.
// |   complete   |   running   |   pending   |
//                ^ kRunningIdx ^ kPendingIdx ^ kQueue.length
// kRunningIdx points to the first running element.
// kPendingIdx points to the first pending element.
// This implements a fast queue with an amortized
// time of O(1).
this[y]=[],this[S]=0,this[F]=0,this[$]=A=>EA(this,A),this[X]=A=>gA(this,A)}get pipelining(){return this[M]}set pipelining(A){this[M]=A,this[$](!0)}get[p](){return this[y].length-this[F]}get[f](){return this[F]-this[S]}get[w](){return this[y].length-this[S]}get[D](){return!!this[_]&&!this[m]&&!this[_].destroyed}get[l](){return Boolean(this[_]?.busy(null)||this[w]>=(iA(this)||1)||this[p]>0)}
/* istanbul ignore: only used for test */[u](A){cA(this),this.once("connect",A)}[q](A,e){const t=A.origin||this[Q].origin,n=new s(t,A,e);return this[y].push(n),this[d]||(null==r.bodyLength(n.body)&&r.isIterable(n.body)?(
// Wait a tick in case stream/iterator is ended in the same tick.
this[d]=1,queueMicrotask(()=>EA(this))):this[$](!0)),this[d]&&2!==this[k]&&this[l]&&(this[k]=2),this[k]<2}async[O](){
// TODO: for H2 we need to gracefully flush the remaining enqueued
// request and close each stream.
return new Promise(A=>{this[w]?this[sA]=A:A(null)})}async[P](A){return new Promise(e=>{const t=this[y].splice(this[F]);for(let e=0;e<t.length;e++){const n=t[e];r.errorRequest(this,n,A)}const n=()=>{this[sA]&&(
// TODO (fix): Should we error here with ClientDestroyedError?
this[sA](),this[sA]=null),e(null)};this[_]?(this[_].destroy(A,n),this[_]=null):queueMicrotask(n),this[$]()})}},te}
/* eslint-disable */function xe(){if(se)return ne;se=1;
// Extracted from node/lib/internal/fixed_queue.js
// Currently optimal queue size, tested on V8 6.0 - 6.6. Must be power of two.
const A=2047;
// The FixedQueue is implemented as a singly-linked list of fixed-size
// circular buffers. It looks something like this:
//  head                                                       tail
//    |                                                          |
//    v                                                          v
// +-----------+ <-----\       +-----------+ <------\         +-----------+
// |  [null]   |        \----- |   next    |         \------- |   next    |
// +-----------+               +-----------+                  +-----------+
// |   item    | <-- bottom    |   item    | <-- bottom       |  [empty]  |
// |   item    |               |   item    |                  |  [empty]  |
// |   item    |               |   item    |                  |  [empty]  |
// |   item    |               |   item    |                  |  [empty]  |
// |   item    |               |   item    |       bottom --> |   item    |
// |   item    |               |   item    |                  |   item    |
// |    ...    |               |    ...    |                  |    ...    |
// |   item    |               |   item    |                  |   item    |
// |   item    |               |   item    |                  |   item    |
// |  [empty]  | <-- top       |   item    |                  |   item    |
// |  [empty]  |               |   item    |                  |   item    |
// |  [empty]  |               |  [empty]  | <-- top  top --> |  [empty]  |
// +-----------+               +-----------+                  +-----------+
// Or, if there is only one circular buffer, it looks something
// like either of these:
//  head   tail                                 head   tail
//    |     |                                     |     |
//    v     v                                     v     v
// +-----------+                               +-----------+
// |  [null]   |                               |  [null]   |
// +-----------+                               +-----------+
// |  [empty]  |                               |   item    |
// |  [empty]  |                               |   item    |
// |   item    | <-- bottom            top --> |  [empty]  |
// |   item    |                               |  [empty]  |
// |  [empty]  | <-- top            bottom --> |   item    |
// |  [empty]  |                               |   item    |
// +-----------+                               +-----------+
// Adding a value means moving `top` forward by one, removing means
// moving `bottom` forward by one. After reaching the end, the queue
// wraps around.
// When `top === bottom` the current queue is empty and when
// `top + 1 === bottom` it's full. This wastes a single space of storage
// but allows much quicker checks.
class e{constructor(){this.bottom=0,this.top=0,this.list=new Array(2048),this.next=null}isEmpty(){return this.top===this.bottom}isFull(){return(this.top+1&A)===this.bottom}push(e){this.list[this.top]=e,this.top=this.top+1&A}shift(){const e=this.list[this.bottom];return void 0===e?null:(this.list[this.bottom]=void 0,this.bottom=this.bottom+1&A,e)}}return ne=class{constructor(){this.head=this.tail=new e}isEmpty(){return this.head.isEmpty()}push(A){this.head.isFull()&&(
// Head is full: Creates a new queue, sets the old queue's `.next` to it,
// and sets it as the new main queue.
this.head=this.head.next=new e),this.head.push(A)}shift(){const A=this.tail,e=A.shift();return A.isEmpty()&&null!==A.next&&(
// If there is another queue, it forms the new tail.
this.tail=A.next),e}}}function We(){if(ge)return ae;ge=1;const A=dA(),e=xe(),{kConnected:t,kSize:r,kRunning:n,kPending:s,kQueued:o,kBusy:i,kFree:a,kUrl:g,kClose:c,kDestroy:Q,kDispatch:E}=BA(),B=function(){if(ie)return oe;ie=1;const{kFree:A,kConnected:e,kPending:t,kQueued:r,kRunning:n,kSize:s}=BA(),o=Symbol("pool");return oe=class{constructor(A){this[o]=A}get connected(){return this[o][e]}get free(){return this[o][A]}get pending(){return this[o][t]}get queued(){return this[o][r]}get running(){return this[o][n]}get size(){return this[o][s]}},oe}(),h=Symbol("clients"),I=Symbol("needDrain"),C=Symbol("queue"),l=Symbol("closed resolve"),u=Symbol("onDrain"),d=Symbol("onConnect"),f=Symbol("onDisconnect"),p=Symbol("onConnectionError"),w=Symbol("get dispatcher"),y=Symbol("add client"),D=Symbol("remove client"),m=Symbol("stats");return ae={PoolBase:class extends A{constructor(A){super(A),this[C]=new e,this[h]=[],this[o]=0;const t=this;this[u]=function(A,e){const r=t[C];let n=!1;for(;!n;){const A=r.shift();if(!A)break;t[o]--,n=!this.dispatch(A.opts,A.handler)}this[I]=n,!this[I]&&t[I]&&(t[I]=!1,t.emit("drain",A,[t,...e])),t[l]&&r.isEmpty()&&Promise.all(t[h].map(A=>A.close())).then(t[l])},this[d]=(A,e)=>{t.emit("connect",A,[t,...e])},this[f]=(A,e,r)=>{t.emit("disconnect",A,[t,...e],r)},this[p]=(A,e,r)=>{t.emit("connectionError",A,[t,...e],r)},this[m]=new B(this)}get[i](){return this[I]}get[t](){return this[h].filter(A=>A[t]).length}get[a](){return this[h].filter(A=>A[t]&&!A[I]).length}get[s](){let A=this[o];for(const{[s]:e}of this[h])A+=e;return A}get[n](){let A=0;for(const{[n]:e}of this[h])A+=e;return A}get[r](){let A=this[o];for(const{[r]:e}of this[h])A+=e;return A}get stats(){return this[m]}async[c](){this[C].isEmpty()?await Promise.all(this[h].map(A=>A.close())):await new Promise(A=>{this[l]=A})}async[Q](A){for(;;){const e=this[C].shift();if(!e)break;e.handler.onError(A)}await Promise.all(this[h].map(e=>e.destroy(A)))}[E](A,e){const t=this[w]();return t?t.dispatch(A,e)||(t[I]=!0,this[I]=!this[w]()):(this[I]=!0,this[C].push({opts:A,handler:e}),this[o]++),!this[I]}[y](A){return A.on("drain",this[u]).on("connect",this[d]).on("disconnect",this[f]).on("connectionError",this[p]),this[h].push(A),this[I]&&queueMicrotask(()=>{this[I]&&this[u](A[g],[this,A])}),this}[D](A){A.close(()=>{const e=this[h].indexOf(A);-1!==e&&this[h].splice(e,1)}),this[I]=this[h].some(A=>!A[I]&&!0!==A.closed&&!0!==A.destroyed)}},kClients:h,kNeedDrain:I,kAddClient:y,kRemoveClient:D,kGetDispatcher:w},ae}function Oe(){if(Qe)return ce;Qe=1;const{PoolBase:A,kClients:e,kNeedDrain:t,kAddClient:r,kGetDispatcher:n}=We(),s=Ve(),{InvalidArgumentError:o}=hA(),i=CA(),{kUrl:a,kInterceptors:g}=BA(),c=pA(),Q=Symbol("options"),E=Symbol("connections"),B=Symbol("factory");function h(A,e){return new s(A,e)}return ce=class extends A{constructor(A,{connections:t,factory:r=h,connect:n,connectTimeout:s,tls:I,maxCachedSessions:C,socketPath:l,autoSelectFamily:u,autoSelectFamilyAttemptTimeout:d,allowH2:f,...p}={}){if(null!=t&&(!Number.isFinite(t)||t<0))throw new o("invalid connections");if("function"!=typeof r)throw new o("factory must be a function.");if(null!=n&&"function"!=typeof n&&"object"!=typeof n)throw new o("connect must be a function or an object");"function"!=typeof n&&(n=c({...I,maxCachedSessions:C,allowH2:f,socketPath:l,timeout:s,...u?{autoSelectFamily:u,autoSelectFamilyAttemptTimeout:d}:void 0,...n})),super(p),this[g]=p.interceptors?.Pool&&Array.isArray(p.interceptors.Pool)?p.interceptors.Pool:[],this[E]=t||null,this[a]=i.parseOrigin(A),this[Q]={...i.deepClone(p),connect:n,allowH2:f},this[Q].interceptors=p.interceptors?{...p.interceptors}:void 0,this[B]=r,this.on("connectionError",(A,t,r)=>{
// If a connection error occurs, we remove the client from the pool,
// and emit a connectionError event. They will not be re-used.
// Fixes https://github.com/nodejs/undici/issues/3895
for(const A of t){
// Do not use kRemoveClient here, as it will close the client,
// but the client cannot be closed in this state.
const t=this[e].indexOf(A);-1!==t&&this[e].splice(t,1)}})}[n](){for(const A of this[e])if(!A[t])return A;if(!this[E]||this[e].length<this[E]){const A=this[B](this[a],this[Q]);return this[r](A),A}}},ce}function Pe(){if(Ie)return he;Ie=1;const{InvalidArgumentError:A}=hA(),{kClients:e,kRunning:t,kClose:r,kDestroy:n,kDispatch:s,kInterceptors:o}=BA(),i=dA(),a=Oe(),g=Ve(),c=CA(),Q=He(),E=Symbol("onConnect"),B=Symbol("onDisconnect"),h=Symbol("onConnectionError"),I=Symbol("maxRedirections"),C=Symbol("onDrain"),l=Symbol("factory"),u=Symbol("options");function d(A,e){return e&&1===e.connections?new g(A,e):new a(A,e)}return he=class extends i{constructor({factory:t=d,maxRedirections:r=0,connect:n,...s}={}){if("function"!=typeof t)throw new A("factory must be a function.");if(null!=n&&"function"!=typeof n&&"object"!=typeof n)throw new A("connect must be a function or an object");if(!Number.isInteger(r)||r<0)throw new A("maxRedirections must be a positive number");super(s),n&&"function"!=typeof n&&(n={...n}),this[o]=s.interceptors?.Agent&&Array.isArray(s.interceptors.Agent)?s.interceptors.Agent:[Q({maxRedirections:r})],this[u]={...c.deepClone(s),connect:n},this[u].interceptors=s.interceptors?{...s.interceptors}:void 0,this[I]=r,this[l]=t,this[e]=new Map,this[C]=(A,e)=>{this.emit("drain",A,[this,...e])},this[E]=(A,e)=>{this.emit("connect",A,[this,...e])},this[B]=(A,e,t)=>{this.emit("disconnect",A,[this,...e],t)},this[h]=(A,e,t)=>{this.emit("connectionError",A,[this,...e],t)}}get[t](){let A=0;for(const r of this[e].values())A+=r[t];return A}[s](t,r){let n;if(!t.origin||!("string"==typeof t.origin||t.origin instanceof URL))throw new A("opts.origin must be a non-empty string or URL.");n=String(t.origin);let s=this[e].get(n);return s||(s=this[l](t.origin,this[u]).on("drain",this[C]).on("connect",this[E]).on("disconnect",this[B]).on("connectionError",this[h]),
// This introduces a tiny memory leak, as dispatchers are never removed from the map.
// TODO(mcollina): remove te timer when the client/pool do not have any more
// active connections.
this[e].set(n,s)),s.dispatch(t,r)}async[r](){const A=[];for(const t of this[e].values())A.push(t.close());this[e].clear(),await Promise.all(A)}async[n](A){const t=[];for(const r of this[e].values())t.push(r.destroy(A));this[e].clear(),await Promise.all(t)}},he}function qe(){if(le)return Ce;le=1;const{kProxy:A,kClose:e,kDestroy:t,kDispatch:r,kInterceptors:n}=BA(),{URL:s}=k,o=Pe(),i=Oe(),a=dA(),{InvalidArgumentError:g,RequestAbortedError:c,SecureProxyConnectionError:Q}=hA(),E=pA(),B=Ve(),h=Symbol("proxy agent"),I=Symbol("proxy client"),C=Symbol("proxy headers"),l=Symbol("request tls settings"),u=Symbol("proxy tls settings"),d=Symbol("connect endpoint function"),f=Symbol("tunnel proxy");function p(A,e){return new i(A,e)}const w=()=>{};function y(A,e){return 1===e.connections?new B(A,e):new i(A,e)}class D extends a{#a;constructor(A,{headers:e={},connect:t,factory:r}){if(super(),!A)throw new g("Proxy URL is mandatory");this[C]=e,this.#a=r?r(A,{connect:t}):new B(A,{connect:t})}[r](A,e){const t=e.onHeaders;e.onHeaders=function(A,r,n){407!==A?t&&t.call(this,A,r,n):"function"==typeof e.onError&&e.onError(new g("Proxy Authentication Required (407)"))};
// Rewrite request as an HTTP1 Proxy request, without tunneling.
const{origin:n,path:o="/",headers:i={}}=A;if(A.path=n+o,!("host"in i)&&!("Host"in i)){const{host:A}=new s(n);i.host=A}return A.headers={...this[C],...i},this.#a[r](A,e)}async[e](){return this.#a.close()}async[t](A){return this.#a.destroy(A)}}return Ce=class extends a{constructor(e){if(super(),!e||"object"==typeof e&&!(e instanceof s)&&!e.uri)throw new g("Proxy uri is mandatory");const{clientFactory:t=p}=e;if("function"!=typeof t)throw new g("Proxy opts.clientFactory must be a function.");const{proxyTunnel:r=!0}=e,i=this.#g(e),{href:a,origin:B,port:m,protocol:k,username:R,password:b,hostname:F}=i;if(this[A]={uri:a,protocol:k},this[n]=e.interceptors?.ProxyAgent&&Array.isArray(e.interceptors.ProxyAgent)?e.interceptors.ProxyAgent:[],this[l]=e.requestTls,this[u]=e.proxyTls,this[C]=e.headers||{},this[f]=r,e.auth&&e.token)throw new g("opts.auth cannot be used in combination with opts.token");e.auth?
/* @deprecated in favour of opts.token */
this[C]["proxy-authorization"]=`Basic ${e.auth}`:e.token?this[C]["proxy-authorization"]=e.token:R&&b&&(this[C]["proxy-authorization"]=`Basic ${Buffer.from(`${decodeURIComponent(R)}:${decodeURIComponent(b)}`).toString("base64")}`);const S=E({...e.proxyTls});this[d]=E({...e.requestTls});const N=e.factory||y;this[I]=t(i,{connect:S}),this[h]=new o({...e,factory:(e,t)=>{const{protocol:r}=new s(e);return this[f]||"http:"!==r||"http:"!==this[A].protocol?N(e,t):new D(this[A].uri,{headers:this[C],connect:S,factory:N})},connect:async(A,e)=>{let t=A.host;A.port||(t+=`:${function(A){return"https:"===A?443:80}(A.protocol)}`);try{const{socket:r,statusCode:n}=await this[I].connect({origin:B,port:m,path:t,signal:A.signal,headers:{...this[C],host:A.host},servername:this[u]?.servername||F});if(200!==n&&(r.on("error",w).destroy(),e(new c(`Proxy response (${n}) !== 200 when HTTP Tunneling`))),"https:"!==A.protocol)return void e(null,r);let s;s=this[l]?this[l].servername:A.servername,this[d]({...A,servername:s,httpSocket:r},e)}catch(A){"ERR_TLS_CERT_ALTNAME_INVALID"===A.code?
// Throw a custom error to avoid loop in client.js#connect
e(new Q(A)):e(A)}}})}dispatch(A,e){const t=
/**
	 * @param {string[] | Record<string, string>} headers
	 * @returns {Record<string, string>}
	 */
function(A){
// When using undici.fetch, the headers list is stored
// as an array.
if(Array.isArray(A)){
/** @type {Record<string, string>} */
const e={};for(let t=0;t<A.length;t+=2)e[A[t]]=A[t+1];return e}return A}
/**
	 * @param {Record<string, string>} headers
	 *
	 * Previous versions of ProxyAgent suggests the Proxy-Authorization in request headers
	 * Nevertheless, it was changed and to avoid a security vulnerability by end users
	 * this check was created.
	 * It should be removed in the next major version for performance reasons
	 */(A.headers);if(function(A){const e=A&&Object.keys(A).find(A=>"proxy-authorization"===A.toLowerCase());if(e)throw new g("Proxy-Authorization should be sent in ProxyAgent constructor")}(t),t&&!("host"in t)&&!("Host"in t)){const{host:e}=new s(A.origin);t.host=e}return this[h].dispatch({...A,headers:t},e)}
/**
	   * @param {import('../types/proxy-agent').ProxyAgent.Options | string | URL} opts
	   * @returns {URL}
	   */#g(A){return"string"==typeof A?new s(A):A instanceof s?A:new s(A.uri)}async[e](){await this[h].close(),await this[I].close()}async[t](){await this[h].destroy(),await this[I].destroy()}},Ce}function Ze(){if(pe)return fe;pe=1;const A=E,{kRetryHandlerDefaultRetry:e}=BA(),{RequestRetryError:t}=hA(),{isDisturbed:r,parseHeaders:n,parseRangeHeader:s,wrapRequestBody:o}=CA();function i(A,e,r,n){const s=A["content-length"];if(null==s)return null;if(!Number.isFinite(e.start)||!Number.isFinite(e.end))return null;const o=Number(s),i=e.end-e.start+1;return Number.isFinite(o)&&o===i?null:new t("Content-Length mismatch",r,{headers:A,data:{count:n}})}class a{constructor(A,t){const{retryOptions:r,...n}=A,{
// Retry scoped
retry:s,maxRetries:i,maxTimeout:g,minTimeout:c,timeoutFactor:Q,methods:
// Response scoped
E,errorCodes:B,retryAfter:h,statusCodes:I}=r??{};this.dispatch=t.dispatch,this.handler=t.handler,this.opts={...n,body:o(A.body)},this.abort=null,this.aborted=!1,this.retryOpts={retry:s??a[e],retryAfter:h??!0,maxTimeout:g??3e4,// 30s,
minTimeout:c??500,// .5s
timeoutFactor:Q??2,maxRetries:i??5,
// What errors we should retry
methods:E??["GET","HEAD","OPTIONS","PUT","DELETE","TRACE"],
// Indicates which errors to retry
statusCodes:I??[500,502,503,504,429],
// List of errors to retry
errorCodes:B??["ECONNRESET","ECONNREFUSED","ENOTFOUND","ENETDOWN","ENETUNREACH","EHOSTDOWN","EHOSTUNREACH","EPIPE","UND_ERR_SOCKET"]},this.retryCount=0,this.retryCountCheckpoint=0,this.start=0,this.end=null,this.etag=null,this.resume=null,this.headersSent=!1,
// Handle possible onConnect duplication
this.handler.onConnect(A=>{this.aborted=!0,this.abort?this.abort(A):this.reason=A})}checkpointResponseEnd(e,t){if(null==this.end&&"HEAD"!==this.opts.method){const t=e["content-length"];this.end=null!=t?Number(t)-1:null,A(null==this.end||Number.isFinite(this.end),"invalid content-length")}this.resume=null!=this.end?t:null}onRequestSent(){this.handler.onRequestSent&&this.handler.onRequestSent()}onUpgrade(A,e,t){this.handler.onUpgrade&&this.handler.onUpgrade(A,e,t)}onConnect(A){this.aborted?A(this.reason):this.abort=A}onBodySent(A){if(this.handler.onBodySent)return this.handler.onBodySent(A)}static[e](A,{state:e,opts:t},r){const{statusCode:n,code:s,headers:o}=A,{method:i,retryOptions:a}=t,{maxRetries:g,minTimeout:c,maxTimeout:Q,timeoutFactor:E,statusCodes:B,errorCodes:h,methods:I}=a,{counter:C}=e;
// Any code that is not a Undici's originated and allowed to retry
if(s&&"UND_ERR_REQ_RETRY"!==s&&!h.includes(s))return void r(A);
// If a set of method are provided and the current method is not in the list
if(Array.isArray(I)&&!I.includes(i))return void r(A);
// If a set of status code are provided and the current status code is not in the list
if(null!=n&&Array.isArray(B)&&!B.includes(n))return void r(A);
// If we reached the max number of retries
if(C>g)return void r(A);let l=o?.["retry-after"];l&&(l=Number(l),l=Number.isNaN(l)?function(A){const e=Date.now();return new Date(A).getTime()-e}(l):1e3*l);const u=l>0?Math.min(l,Q):Math.min(c*E**(C-1),Q);setTimeout(()=>r(null),u)}onHeaders(e,r,o,a){const g=n(r);if(this.retryCount+=1,e>=300)
// Only expose a response if no earlier attempt has reached the caller.
// Otherwise abort this attempt so the error settles the existing body
// instead of replacing it with a new response.
return this.headersSent||!1!==this.retryOpts.statusCodes.includes(e)?(this.abort(new t("Request failed",e,{headers:g,data:{count:this.retryCount}})),!1):(this.headersSent=!0,this.checkpointResponseEnd(g,o),this.handler.onHeaders(e,r,o,a));
// Checkpoint for resume from where we left it
if(null!=this.resume){
// Only Partial Content 206 supposed to provide Content-Range,
// any other status code that partially consumed the payload
// should not be retry because it would result in downstream
// wrongly concatanete multiple responses.
if(this.resume=null,206!==e&&(this.start>0||200!==e))return this.abort(new t("server does not support the range header and the payload was partially consumed",e,{headers:g,data:{count:this.retryCount}})),!1;const A=s(g["content-range"]);
// If no content range
if(!A)return this.abort(new t("Content-Range mismatch",e,{headers:g,data:{count:this.retryCount}})),!1;
// Let's start with a weak etag check
if(null!=this.etag&&this.etag!==g.etag)return this.abort(new t("ETag mismatch",e,{headers:g,data:{count:this.retryCount}})),!1;const r=i(g,A,e,this.retryCount);if(null!=r)return this.abort(r),!1;const{start:n,size:a,end:c=a-1}=A;return this.start!==n||null!=this.end&&this.end!==c?(this.abort(new t("Content-Range mismatch",e,{headers:g,data:{count:this.retryCount}})),!1):(this.resume=o,!0)}if(null==this.end){if(206===e){
// First time we receive 206
const t=s(g["content-range"]);if(null==t)return this.headersSent=!0,this.handler.onHeaders(e,r,o,a);const n=i(g,t,e,this.retryCount);if(null!=n)return this.abort(n),!1;const{start:c,size:Q,end:E=Q-1}=t;A(null!=c&&Number.isFinite(c),"content-range mismatch"),A(null!=E&&Number.isFinite(E),"invalid content-length"),this.start=c,this.end=E}
// We make our best to checkpoint the body for further range headers
if(null==this.end){const A=g["content-length"];this.end=null!=A?Number(A)-1:null}return A(Number.isFinite(this.start)),A(null==this.end||Number.isFinite(this.end),"invalid content-length"),this.resume=o,this.headersSent=!0,this.etag=null!=g.etag?g.etag:null,
// Weak etags are not useful for comparison nor cache
// for instance not safe to assume if the response is byte-per-byte
// equal
null!=this.etag&&this.etag.startsWith("W/")&&(this.etag=null),this.handler.onHeaders(e,r,o,a)}const c=new t("Request failed",e,{headers:g,data:{count:this.retryCount}});return this.abort(c),!1}onData(A){return this.start+=A.length,this.handler.onData(A)}onComplete(A){return this.retryCount=0,this.handler.onComplete(A)}onError(A){if(this.aborted||r(this.opts.body)||this.headersSent&&null==this.resume)return this.handler.onError(A);
// We reconcile in case of a mix between network errors
// and server error response
this.retryCount-this.retryCountCheckpoint>0?
// We count the difference between the last checkpoint and the current retry count
this.retryCount=this.retryCountCheckpoint+(this.retryCount-this.retryCountCheckpoint):this.retryCount+=1,this.retryOpts.retry(A,{state:{counter:this.retryCount},opts:{retryOptions:this.retryOpts,...this.opts}},function(A){if(null!=A||this.aborted||r(this.opts.body))return this.handler.onError(A);if(0!==this.start){const A={range:`bytes=${this.start}-${this.end??""}`};
// Weak etag check - weak etags will make comparison algorithms never match
null!=this.etag&&(A["if-match"]=this.etag),this.opts={...this.opts,headers:{...this.opts.headers,...A}}}try{this.retryCountCheckpoint=this.retryCount,this.dispatch(this.opts,this)}catch(A){this.handler.onError(A)}}.bind(this))}}return fe=a}var ze,Ke,Xe,_e,je,$e,At,et,tt,rt,nt,st,ot,it,at,gt,ct,Qt,Et,Bt,ht,It,Ct={},lt={exports:{}};function ut(){if(Ke)return ze;Ke=1;const A=E,{Readable:e}=I,{RequestAbortedError:t,NotSupportedError:r,InvalidArgumentError:n,AbortError:s}=hA(),o=CA(),{ReadableStreamFrom:i}=CA(),a=Symbol("kConsume"),g=Symbol("kReading"),c=Symbol("kBody"),Q=Symbol("kAbort"),B=Symbol("kContentType"),h=Symbol("kContentLength"),C=()=>{};
// https://fetch.spec.whatwg.org/#body-unusable
function l(A){return o.isDisturbed(A)||
// https://streams.spec.whatwg.org/#readablestream-locked
function(A){
// Consume is an implicit lock.
return A[c]&&!0===A[c].locked||A[a]}(A)}async function u(e,r){return A(!e[a]),new Promise((A,n)=>{if(l(e)){const A=e._readableState;A.destroyed&&!1===A.closeEmitted?e.on("error",A=>{n(A)}).on("close",()=>{n(new TypeError("unusable"))}):n(A.errored??new TypeError("unusable"))}else queueMicrotask(()=>{e[a]={type:r,stream:e,resolve:A,reject:n,length:0,body:[]},e.on("error",function(A){y(this[a],A)}).on("close",function(){null!==this[a].body&&y(this[a],new t)}),function(A){if(null===A.body)return;const{_readableState:e}=A.stream;if(e.bufferIndex){const t=e.bufferIndex,r=e.buffer.length;for(let n=t;n<r;n++)w(A,e.buffer[n])}else for(const t of e.buffer)w(A,t);e.endEmitted?p(this[a]):A.stream.on("end",function(){p(this[a])});A.stream.resume();for(;null!=A.stream.read(););}
/**
	 * @param {Buffer[]} chunks
	 * @param {number} length
	 */(e[a])})})}function d(A,e){if(0===A.length||0===e)return"";const t=1===A.length?A[0]:Buffer.concat(A,e),r=t.length,n=r>2&&239===t[0]&&187===t[1]&&191===t[2]?3:0;return t.utf8Slice(n,r)}
/**
	 * @param {Buffer[]} chunks
	 * @param {number} length
	 * @returns {Uint8Array}
	 */function f(A,e){if(0===A.length||0===e)return new Uint8Array(0);if(1===A.length)
// fast-path
return new Uint8Array(A[0]);const t=new Uint8Array(Buffer.allocUnsafeSlow(e).buffer);let r=0;for(let e=0;e<A.length;++e){const n=A[e];t.set(n,r),r+=n.length}return t}function p(A){const{type:e,body:t,resolve:r,stream:n,length:s}=A;try{"text"===e?r(d(t,s)):"json"===e?r(JSON.parse(d(t,s))):"arrayBuffer"===e?r(f(t,s).buffer):"blob"===e?r(new Blob(t,{type:n[B]})):"bytes"===e&&r(f(t,s)),y(A)}catch(A){n.destroy(A)}}function w(A,e){A.length+=e.length,A.body.push(e)}function y(A,e){null!==A.body&&(e?A.reject(e):A.resolve(),A.type=null,A.stream=null,A.resolve=null,A.reject=null,A.length=0,A.body=null)}return ze={Readable:class extends e{constructor({resume:A,abort:e,contentType:t="",contentLength:r,highWaterMark:n=65536}){super({autoDestroy:!0,read:A,highWaterMark:n}),this._readableState.dataEmitted=!1,this[Q]=e,this[a]=null,this[c]=null,this[B]=t,this[h]=r,
// Is stream being consumed through Readable API?
// This is an optimization so that we avoid checking
// for 'data' and 'readable' listeners in the hot path
// inside push().
this[g]=!1}destroy(A){return A||this._readableState.endEmitted||(A=new t),A&&this[Q](),super.destroy(A)}_destroy(A,e){
// Workaround for Node "bug". If the stream is destroyed in same
// tick as it is created, then a user who is waiting for a
// promise (i.e micro tick) for installing a 'error' listener will
// never get a chance and will always encounter an unhandled exception.
this[g]?e(A):setImmediate(()=>{e(A)})}on(A,...e){return"data"!==A&&"readable"!==A||(this[g]=!0),super.on(A,...e)}addListener(A,...e){return this.on(A,...e)}off(A,...e){const t=super.off(A,...e);return"data"!==A&&"readable"!==A||(this[g]=this.listenerCount("data")>0||this.listenerCount("readable")>0),t}removeListener(A,...e){return this.off(A,...e)}push(A){return this[a]&&null!==A?(w(this[a],A),!this[g]||super.push(A)):super.push(A)}
// https://fetch.spec.whatwg.org/#dom-body-text
async text(){return u(this,"text")}
// https://fetch.spec.whatwg.org/#dom-body-json
async json(){return u(this,"json")}
// https://fetch.spec.whatwg.org/#dom-body-blob
async blob(){return u(this,"blob")}
// https://fetch.spec.whatwg.org/#dom-body-bytes
async bytes(){return u(this,"bytes")}
// https://fetch.spec.whatwg.org/#dom-body-arraybuffer
async arrayBuffer(){return u(this,"arrayBuffer")}
// https://fetch.spec.whatwg.org/#dom-body-formdata
async formData(){
// TODO: Implement.
throw new r}
// https://fetch.spec.whatwg.org/#dom-body-bodyused
get bodyUsed(){return o.isDisturbed(this)}
// https://fetch.spec.whatwg.org/#dom-body-body
get body(){return this[c]||(this[c]=i(this),this[a]&&(
// TODO: Is this the best way to force a lock?
this[c].getReader(),// Ensure stream is locked.
A(this[c].locked))),this[c]}async dump(A){let e=Number.isFinite(A?.limit)?A.limit:131072;const t=A?.signal;if(null!=t&&("object"!=typeof t||!("aborted"in t)))throw new n("signal must be an AbortSignal");return t?.throwIfAborted(),this._readableState.closeEmitted?null:await new Promise((A,r)=>{this[h]>e&&this.destroy(new s);const n=()=>{this.destroy(t.reason??new s)};t?.addEventListener("abort",n),this.on("close",function(){t?.removeEventListener("abort",n),t?.aborted?r(t.reason??new s):A(null)}).on("error",C).on("data",function(A){e-=A.length,e<=0&&this.destroy()}).resume()})}},chunksDecode:d}}function dt(){if(_e)return Xe;_e=1;const A=E,{ResponseStatusCodeError:e}=hA(),{chunksDecode:t}=ut();const r=A=>A.length>15&&"/"===A[11]&&"a"===A[0]&&"p"===A[1]&&"p"===A[2]&&"l"===A[3]&&"i"===A[4]&&"c"===A[5]&&"a"===A[6]&&"t"===A[7]&&"i"===A[8]&&"o"===A[9]&&"n"===A[10]&&"j"===A[12]&&"s"===A[13]&&"o"===A[14]&&"n"===A[15],n=A=>A.length>4&&"/"===A[4]&&"t"===A[0]&&"e"===A[1]&&"x"===A[2]&&"t"===A[3];return Xe={getResolveErrorBodyCallback:async function({callback:s,body:o,contentType:i,statusCode:a,statusMessage:g,headers:c}){A(o);let Q=[],E=0;try{for await(const A of o)if(Q.push(A),E+=A.length,E>131072){Q=[],E=0;break}}catch{Q=[],E=0}const B=`Response status code ${a}${g?`: ${g}`:""}`;if(204===a||!i||!E)return void queueMicrotask(()=>s(new e(B,a,c)));const h=Error.stackTraceLimit;let I;Error.stackTraceLimit=0;try{r(i)?I=JSON.parse(t(Q,E)):n(i)&&(I=t(Q,E))}catch{
// process in a callback to avoid throwing in the microtask queue
}finally{Error.stackTraceLimit=h}queueMicrotask(()=>s(new e(B,a,c,I)))},isContentTypeApplicationJson:r,isContentTypeText:n},Xe}function ft(){if(At)return $e;At=1;const{addAbortListener:A}=CA(),{RequestAbortedError:e}=hA(),t=Symbol("kListener"),r=Symbol("kSignal");function n(A){A.abort?A.abort(A[r]?.reason):A.reason=A[r]?.reason??new e,s(A)}function s(A){A[r]&&("removeEventListener"in A[r]?A[r].removeEventListener("abort",A[t]):A[r].removeListener("abort",A[t]),A[r]=null,A[t]=null)}return $e={addSignal:function(e,s){e.reason=null,e[r]=null,e[t]=null,s&&(s.aborted?n(e):(e[r]=s,e[t]=()=>{n(e)},A(e[r],e[t])))},removeSignal:s}}function pt(){return gt||(gt=1,Ct.request=function(){if(je)return lt.exports;je=1;const A=E,{Readable:e}=ut(),{InvalidArgumentError:t,RequestAbortedError:r}=hA(),n=CA(),{getResolveErrorBodyCallback:s}=dt(),{AsyncResource:o}=R;class i extends o{constructor(A,e){if(!A||"object"!=typeof A)throw new t("invalid opts");const{signal:s,method:o,opaque:i,body:a,onInfo:g,responseHeaders:c,throwOnError:Q,highWaterMark:E}=A;try{if("function"!=typeof e)throw new t("invalid callback");if(E&&("number"!=typeof E||E<0))throw new t("invalid highWaterMark");if(s&&"function"!=typeof s.on&&"function"!=typeof s.addEventListener)throw new t("signal must be an EventEmitter or EventTarget");if("CONNECT"===o)throw new t("invalid method");if(g&&"function"!=typeof g)throw new t("invalid onInfo callback");super("UNDICI_REQUEST")}catch(A){throw n.isStream(a)&&n.destroy(a.on("error",n.nop),A),A}this.method=o,this.responseHeaders=c||null,this.opaque=i||null,this.callback=e,this.res=null,this.abort=null,this.body=a,this.trailers={},this.context=null,this.onInfo=g||null,this.throwOnError=Q,this.highWaterMark=E,this.signal=s,this.reason=null,this.removeAbortListener=null,n.isStream(a)&&a.on("error",A=>{this.onError(A)}),this.signal&&(this.signal.aborted?this.reason=this.signal.reason??new r:this.removeAbortListener=n.addAbortListener(this.signal,()=>{this.reason=this.signal.reason??new r,this.res?n.destroy(this.res.on("error",n.nop),this.reason):this.abort&&this.abort(this.reason),this.removeAbortListener&&(this.res?.off("close",this.removeAbortListener),this.removeAbortListener(),this.removeAbortListener=null)}))}onConnect(e,t){this.reason?e(this.reason):(A(this.callback),this.abort=e,this.context=t)}onHeaders(A,t,r,o){const{callback:i,opaque:a,abort:g,context:c,responseHeaders:Q,highWaterMark:E}=this,B="raw"===Q?n.parseRawHeaders(t):n.parseHeaders(t);if(A<200)return void(this.onInfo&&this.onInfo({statusCode:A,headers:B}));const h="raw"===Q?n.parseHeaders(t):B,I=h["content-type"],C=h["content-length"],l=new e({resume:r,abort:g,contentType:I,contentLength:"HEAD"!==this.method&&C?Number(C):null,highWaterMark:E});this.removeAbortListener&&l.on("close",this.removeAbortListener),this.callback=null,this.res=l,null!==i&&(this.throwOnError&&A>=400?this.runInAsyncScope(s,null,{callback:i,body:l,contentType:I,statusCode:A,statusMessage:o,headers:B}):this.runInAsyncScope(i,null,null,{statusCode:A,headers:B,trailers:this.trailers,opaque:a,body:l,context:c}))}onData(A){return this.res.push(A)}onComplete(A){n.parseHeaders(A,this.trailers),this.res.push(null)}onError(A){const{res:e,callback:t,body:r,opaque:s}=this;t&&(
// TODO: Does this need queueMicrotask?
this.callback=null,queueMicrotask(()=>{this.runInAsyncScope(t,null,A,{opaque:s})})),e&&(this.res=null,
// Ensure all queued handlers are invoked before destroying res.
queueMicrotask(()=>{n.destroy(e,A)})),r&&(this.body=null,n.destroy(r,A)),this.removeAbortListener&&(e?.off("close",this.removeAbortListener),this.removeAbortListener(),this.removeAbortListener=null)}}return lt.exports=function A(e,t){if(void 0===t)return new Promise((t,r)=>{A.call(this,e,(A,e)=>A?r(A):t(e))});try{this.dispatch(e,new i(e,t))}catch(A){if("function"!=typeof t)throw A;const r=e?.opaque;queueMicrotask(()=>t(A,{opaque:r}))}},lt.exports.RequestHandler=i,lt.exports}(),Ct.stream=function(){if(tt)return et;tt=1;const A=E,{finished:e,PassThrough:t}=I,{InvalidArgumentError:r,InvalidReturnValueError:n}=hA(),s=CA(),{getResolveErrorBodyCallback:o}=dt(),{AsyncResource:i}=R,{addSignal:a,removeSignal:g}=ft();class c extends i{constructor(A,e,t){if(!A||"object"!=typeof A)throw new r("invalid opts");const{signal:n,method:o,opaque:i,body:g,onInfo:c,responseHeaders:Q,throwOnError:E}=A;try{if("function"!=typeof t)throw new r("invalid callback");if("function"!=typeof e)throw new r("invalid factory");if(n&&"function"!=typeof n.on&&"function"!=typeof n.addEventListener)throw new r("signal must be an EventEmitter or EventTarget");if("CONNECT"===o)throw new r("invalid method");if(c&&"function"!=typeof c)throw new r("invalid onInfo callback");super("UNDICI_STREAM")}catch(A){throw s.isStream(g)&&s.destroy(g.on("error",s.nop),A),A}this.responseHeaders=Q||null,this.opaque=i||null,this.factory=e,this.callback=t,this.res=null,this.abort=null,this.context=null,this.trailers=null,this.body=g,this.onInfo=c||null,this.throwOnError=E||!1,s.isStream(g)&&g.on("error",A=>{this.onError(A)}),a(this,n)}onConnect(e,t){this.reason?e(this.reason):(A(this.callback),this.abort=e,this.context=t)}onHeaders(A,r,i,a){const{factory:g,opaque:c,context:Q,callback:E,responseHeaders:B}=this,h="raw"===B?s.parseRawHeaders(r):s.parseHeaders(r);if(A<200)return void(this.onInfo&&this.onInfo({statusCode:A,headers:h}));let I;if(this.factory=null,this.throwOnError&&A>=400){const e=("raw"===B?s.parseHeaders(r):h)["content-type"];I=new t,this.callback=null,this.runInAsyncScope(o,null,{callback:E,body:I,contentType:e,statusCode:A,statusMessage:a,headers:h})}else{if(null===g)return;if(I=this.runInAsyncScope(g,null,{statusCode:A,headers:h,opaque:c,context:Q}),!I||"function"!=typeof I.write||"function"!=typeof I.end||"function"!=typeof I.on)throw new n("expected Writable");
// TODO: Avoid finished. It registers an unnecessary amount of listeners.
e(I,{readable:!1},A=>{const{callback:e,res:t,opaque:r,trailers:n,abort:o}=this;this.res=null,!A&&t.readable||s.destroy(t,A),this.callback=null,this.runInAsyncScope(e,null,A||null,{opaque:r,trailers:n}),A&&o()})}return I.on("drain",i),this.res=I,!0!==(void 0!==I.writableNeedDrain?I.writableNeedDrain:I._writableState?.needDrain)}onData(A){const{res:e}=this;return!e||e.write(A)}onComplete(A){const{res:e}=this;g(this),e&&(this.trailers=s.parseHeaders(A),e.end())}onError(A){const{res:e,callback:t,opaque:r,body:n}=this;g(this),this.factory=null,e?(this.res=null,s.destroy(e,A)):t&&(this.callback=null,queueMicrotask(()=>{this.runInAsyncScope(t,null,A,{opaque:r})})),n&&(this.body=null,s.destroy(n,A))}}return et=function A(e,t,r){if(void 0===r)return new Promise((r,n)=>{A.call(this,e,t,(A,e)=>A?n(A):r(e))});try{this.dispatch(e,new c(e,t,r))}catch(A){if("function"!=typeof r)throw A;const t=e?.opaque;queueMicrotask(()=>r(A,{opaque:t}))}}}(),Ct.pipeline=function(){if(nt)return rt;nt=1;const{Readable:A,Duplex:e,PassThrough:t}=I,{InvalidArgumentError:r,InvalidReturnValueError:n,RequestAbortedError:s}=hA(),o=CA(),{AsyncResource:i}=R,{addSignal:a,removeSignal:g}=ft(),c=E,Q=Symbol("resume");class B extends A{constructor(){super({autoDestroy:!0}),this[Q]=null}_read(){const{[Q]:A}=this;A&&(this[Q]=null,A())}_destroy(A,e){this._read(),e(A)}}class h extends A{constructor(A){super({autoDestroy:!0}),this[Q]=A}_read(){this[Q]()}_destroy(A,e){A||this._readableState.endEmitted||(A=new s),e(A)}}class C extends i{constructor(A,t){if(!A||"object"!=typeof A)throw new r("invalid opts");if("function"!=typeof t)throw new r("invalid handler");const{signal:n,method:i,opaque:c,onInfo:E,responseHeaders:h}=A;if(n&&"function"!=typeof n.on&&"function"!=typeof n.addEventListener)throw new r("signal must be an EventEmitter or EventTarget");if("CONNECT"===i)throw new r("invalid method");if(E&&"function"!=typeof E)throw new r("invalid onInfo callback");super("UNDICI_PIPELINE"),this.opaque=c||null,this.responseHeaders=h||null,this.handler=t,this.abort=null,this.context=null,this.onInfo=E||null,this.req=(new B).on("error",o.nop),this.ret=new e({readableObjectMode:A.objectMode,autoDestroy:!0,read:()=>{const{body:A}=this;A?.resume&&A.resume()},write:(A,e,t)=>{const{req:r}=this;r.push(A,e)||r._readableState.destroyed?t():r[Q]=t},destroy:(A,e)=>{const{body:t,req:r,res:n,ret:i,abort:a}=this;A||i._readableState.endEmitted||(A=new s),a&&A&&a(),o.destroy(t,A),o.destroy(r,A),o.destroy(n,A),g(this),e(A)}}).on("prefinish",()=>{const{req:A}=this;
// Node < 15 does not call _final in same tick.
A.push(null)}),this.res=null,a(this,n)}onConnect(A,e){const{ret:t,res:r}=this;this.reason?A(this.reason):(c(!r,"pipeline cannot be retried"),c(!t.destroyed),this.abort=A,this.context=e)}onHeaders(A,e,t){const{opaque:r,handler:i,context:a}=this;if(A<200){if(this.onInfo){const t="raw"===this.responseHeaders?o.parseRawHeaders(e):o.parseHeaders(e);this.onInfo({statusCode:A,headers:t})}return}let g;this.res=new h(t);try{this.handler=null;const t="raw"===this.responseHeaders?o.parseRawHeaders(e):o.parseHeaders(e);g=this.runInAsyncScope(i,null,{statusCode:A,headers:t,opaque:r,body:this.res,context:a})}catch(A){throw this.res.on("error",o.nop),A}if(!g||"function"!=typeof g.on)throw new n("expected Readable");g.on("data",A=>{const{ret:e,body:t}=this;!e.push(A)&&t.pause&&t.pause()}).on("error",A=>{const{ret:e}=this;o.destroy(e,A)}).on("end",()=>{const{ret:A}=this;A.push(null)}).on("close",()=>{const{ret:A}=this;A._readableState.ended||o.destroy(A,new s)}),this.body=g}onData(A){const{res:e}=this;return e.push(A)}onComplete(A){const{res:e}=this;e.push(null)}onError(A){const{ret:e}=this;this.handler=null,o.destroy(e,A)}}return rt=function(A,e){try{const t=new C(A,e);return this.dispatch({...A,body:t.req},t),t.ret}catch(A){return(new t).destroy(A)}}}(),Ct.upgrade=function(){if(ot)return st;ot=1;const{InvalidArgumentError:A,SocketError:e}=hA(),{AsyncResource:t}=R,r=CA(),{addSignal:n,removeSignal:s}=ft(),o=E;class i extends t{constructor(e,t){if(!e||"object"!=typeof e)throw new A("invalid opts");if("function"!=typeof t)throw new A("invalid callback");const{signal:r,opaque:s,responseHeaders:o}=e;if(r&&"function"!=typeof r.on&&"function"!=typeof r.addEventListener)throw new A("signal must be an EventEmitter or EventTarget");super("UNDICI_UPGRADE"),this.responseHeaders=o||null,this.opaque=s||null,this.callback=t,this.abort=null,this.context=null,n(this,r)}onConnect(A,e){this.reason?A(this.reason):(o(this.callback),this.abort=A,this.context=null)}onHeaders(){throw new e("bad upgrade",null)}onUpgrade(A,e,t){o(101===A);const{callback:n,opaque:i,context:a}=this;s(this),this.callback=null;const g="raw"===this.responseHeaders?r.parseRawHeaders(e):r.parseHeaders(e);this.runInAsyncScope(n,null,null,{headers:g,socket:t,opaque:i,context:a})}onError(A){const{callback:e,opaque:t}=this;s(this),e&&(this.callback=null,queueMicrotask(()=>{this.runInAsyncScope(e,null,A,{opaque:t})}))}}return st=function A(e,t){if(void 0===t)return new Promise((t,r)=>{A.call(this,e,(A,e)=>A?r(A):t(e))});try{const A=new i(e,t);this.dispatch({...e,method:e.method||"GET",upgrade:e.protocol||"Websocket"},A)}catch(A){if("function"!=typeof t)throw A;const r=e?.opaque;queueMicrotask(()=>t(A,{opaque:r}))}}}(),Ct.connect=function(){if(at)return it;at=1;const A=E,{AsyncResource:e}=R,{InvalidArgumentError:t,SocketError:r}=hA(),n=CA(),{addSignal:s,removeSignal:o}=ft();class i extends e{constructor(A,e){if(!A||"object"!=typeof A)throw new t("invalid opts");if("function"!=typeof e)throw new t("invalid callback");const{signal:r,opaque:n,responseHeaders:o}=A;if(r&&"function"!=typeof r.on&&"function"!=typeof r.addEventListener)throw new t("signal must be an EventEmitter or EventTarget");super("UNDICI_CONNECT"),this.opaque=n||null,this.responseHeaders=o||null,this.callback=e,this.abort=null,s(this,r)}onConnect(e,t){this.reason?e(this.reason):(A(this.callback),this.abort=e,this.context=t)}onHeaders(){throw new r("bad connect",null)}onUpgrade(A,e,t){const{callback:r,opaque:s,context:i}=this;o(this),this.callback=null;let a=e;
// Indicates is an HTTP2Session
null!=a&&(a="raw"===this.responseHeaders?n.parseRawHeaders(e):n.parseHeaders(e)),this.runInAsyncScope(r,null,null,{statusCode:A,headers:a,socket:t,opaque:s,context:i})}onError(A){const{callback:e,opaque:t}=this;o(this),e&&(this.callback=null,queueMicrotask(()=>{this.runInAsyncScope(e,null,A,{opaque:t})}))}}return it=function A(e,t){if(void 0===t)return new Promise((t,r)=>{A.call(this,e,(A,e)=>A?r(A):t(e))});try{const A=new i(e,t);this.dispatch({...e,method:"CONNECT"},A)}catch(A){if("function"!=typeof t)throw A;const r=e?.opaque;queueMicrotask(()=>t(A,{opaque:r}))}},it}()),Ct}function wt(){if(Qt)return ct;Qt=1;const{UndiciError:A}=hA(),e=Symbol.for("undici.error.UND_MOCK_ERR_MOCK_NOT_MATCHED");
/**
	 * The request does not match any registered mock dispatches.
	 */
class t extends A{constructor(A){super(A),Error.captureStackTrace(this,t),this.name="MockNotMatchedError",this.message=A||"The request does not match any registered mock dispatches",this.code="UND_MOCK_ERR_MOCK_NOT_MATCHED"}static[Symbol.hasInstance](A){return A&&!0===A[e]}[e]=!0}return ct={MockNotMatchedError:t}}function yt(){return Bt?Et:(Bt=1,Et={kAgent:Symbol("agent"),kOptions:Symbol("options"),kFactory:Symbol("factory"),kDispatches:Symbol("dispatches"),kDispatchKey:Symbol("dispatch key"),kDefaultHeaders:Symbol("default headers"),kDefaultTrailers:Symbol("default trailers"),kContentLength:Symbol("content length"),kMockAgent:Symbol("mock agent"),kMockAgentSet:Symbol("mock agent set"),kMockAgentGet:Symbol("mock agent get"),kMockDispatch:Symbol("mock dispatch"),kClose:Symbol("close"),kOriginalClose:Symbol("original agent close"),kOrigin:Symbol("origin"),kIsMockActive:Symbol("is mock active"),kNetConnect:Symbol("net connect"),kGetNetConnect:Symbol("get net connect"),kConnected:Symbol("connected")})}function Dt(){if(It)return ht;It=1;const{MockNotMatchedError:A}=wt(),{kDispatches:e,kMockAgent:t,kOriginalDispatch:r,kOrigin:n,kGetNetConnect:s}=yt(),{buildURL:o}=CA(),{STATUS_CODES:i}=h,{types:{isPromise:a}}=l;function g(A,e){return"string"==typeof A?A===e:A instanceof RegExp?A.test(e):"function"==typeof A&&!0===A(e)}function c(A){return Object.fromEntries(Object.entries(A).map(([A,e])=>[A.toLocaleLowerCase(),e]))}
/**
	 * @param {import('../../index').Headers|string[]|Record<string, string>} headers
	 * @param {string} key
	 */function Q(A,e){if(!Array.isArray(A))return"function"==typeof A.get?A.get(e):c(A)[e.toLocaleLowerCase()];for(let t=0;t<A.length;t+=2)if(A[t].toLocaleLowerCase()===e.toLocaleLowerCase())return A[t+1]}
/** @param {string[]} headers */function E(A){// fetch HeadersList
const e=A.slice(),t=[];for(let A=0;A<e.length;A+=2)t.push([e[A],e[A+1]]);return Object.fromEntries(t)}function B(A,e){if("function"==typeof A.headers)return Array.isArray(e)&&(// fetch HeadersList
e=E(e)),A.headers(e?c(e):{});if(void 0===A.headers)return!0;if("object"!=typeof e||"object"!=typeof A.headers)return!1;for(const[t,r]of Object.entries(A.headers)){if(!g(r,Q(e,t)))return!1}return!0}function I(A){if("string"!=typeof A)return A;const e=A.split("?");if(2!==e.length)return A;const t=new URLSearchParams(e.pop());return t.sort(),[...e,t.toString()].join("?")}function C(A){return Buffer.isBuffer(A)||A instanceof Uint8Array||A instanceof ArrayBuffer?A:"object"==typeof A?JSON.stringify(A):A.toString()}function u(e,t){const r=t.query?o(t.path,t.query):t.path,n="string"==typeof r?I(r):r;
// Match path
let s=e.filter(({consumed:A})=>!A).filter(({path:A})=>g(I(A),n));if(0===s.length)throw new A(`Mock dispatch not matched for path '${n}'`);
// Match method
if(s=s.filter(({method:A})=>g(A,t.method)),0===s.length)throw new A(`Mock dispatch not matched for method '${t.method}' on path '${n}'`);
// Match body
if(s=s.filter(({body:A})=>void 0===A||g(A,t.body)),0===s.length)throw new A(`Mock dispatch not matched for body '${t.body}' on path '${n}'`);
// Match headers
if(s=s.filter(A=>B(A,t.headers)),0===s.length){const e="object"==typeof t.headers?JSON.stringify(t.headers):t.headers;throw new A(`Mock dispatch not matched for headers '${e}' on path '${n}'`)}return s[0]}function d(A,e){const t=A.findIndex(A=>!!A.consumed&&function(A,{path:e,method:t,body:r,headers:n}){const s=g(A.path,e),o=g(A.method,t),i=void 0===A.body||g(A.body,r),a=B(A,n);return s&&o&&i&&a}(A,e));-1!==t&&A.splice(t,1)}function f(A){const{path:e,method:t,body:r,headers:n,query:s}=A;return{path:e,method:t,body:r,headers:n,query:s}}function p(A){const e=Object.keys(A),t=[];for(let r=0;r<e.length;++r){const n=e[r],s=A[n],o=Buffer.from(`${n}`);if(Array.isArray(s))for(let A=0;A<s.length;++A)t.push(o,Buffer.from(`${s[A]}`));else t.push(o,Buffer.from(`${s}`))}return t}
/**
	 * @see https://developer.mozilla.org/en-US/docs/Web/HTTP/Status
	 * @param {number} statusCode
	 */function w(A){return i[A]||"unknown"}
/**
	 * Mock dispatch function used to simulate undici dispatches
	 */
function y(A,t){
// Get mock dispatch from built key
const r=f(A),n=u(this[e],r);n.timesInvoked++,
// Here's where we resolve a callback if a callback is present for the dispatch data.
n.data.callback&&(n.data={...n.data,...n.data.callback(A)});
// Parse mockDispatch data
const{data:{statusCode:s,data:o,headers:i,trailers:g,error:c},delay:Q,persist:B}=n,{timesInvoked:h,times:I}=n;
// If specified, trigger dispatch error
if(
// If it's used up and not persistent, mark as consumed
n.consumed=!B&&h>=I,n.pending=h<I,null!==c)return d(this[e],r),t.onError(c),!0;
// Handle the request with a delay if necessary
function l(e,n=o){
// fetch's HeadersList is a 1D string array
const c=Array.isArray(A.headers)?E(A.headers):A.headers,Q="function"==typeof n?n({...A,headers:c}):n;
// util.types.isPromise is likely needed for jest.
if(a(Q))
// If handleReply is asynchronous, throwing an error
// in the callback will reject the promise, rather than
// synchronously throw the error, which breaks some tests.
// Rather, we wait for the callback to resolve if it is a
// promise, and then re-run handleReply with the new body.
return void Q.then(A=>l(e,A));const B=C(Q),h=p(i),I=p(g);t.onConnect?.(A=>t.onError(A),null),t.onHeaders?.(s,h,y,w(s)),t.onData?.(Buffer.from(B)),t.onComplete?.(I),d(e,r)}function y(){}return"number"==typeof Q&&Q>0?setTimeout(()=>{l(this[e])},Q):l(this[e]),!0}function D(A,e){const t=new URL(e);return!0===A||!(!Array.isArray(A)||!A.some(A=>g(A,t.host)))}return ht={getResponseData:C,getMockDispatch:u,addMockDispatch:function(A,e,t){const r={timesInvoked:0,times:1,persist:!1,consumed:!1,...e,pending:!0,data:{error:null,..."function"==typeof t?{callback:t}:{...t}}};return A.push(r),r},deleteMockDispatch:d,buildKey:f,generateKeyValues:p,matchValue:g,getResponse:async function(A){const e=[];for await(const t of A)e.push(t);return Buffer.concat(e).toString("utf8")},getStatusText:w,mockDispatch:y,buildMockDispatch:function(){const e=this[t],o=this[n],i=this[r];return function(t,r){if(e.isMockActive)try{y.call(this,t,r)}catch(n){if(!(n instanceof A))throw n;{const a=e[s]();if(!1===a)throw new A(`${n.message}: subsequent request to origin ${o} was not allowed (net.connect disabled)`);if(!D(a,o))throw new A(`${n.message}: subsequent request to origin ${o} was not allowed (net.connect is not enabled for this origin)`);i.call(this,t,r)}}else i.call(this,t,r)}},checkNetConnect:D,buildMockOptions:function(A){if(A){const{agent:e,...t}=A;return t}},getHeaderByName:Q,buildHeadersFromArray:E},ht}var mt,kt,Rt,bt,Ft,St,Nt,Mt,Ut,Lt,Tt,Gt,Yt,vt,Jt,Ht,Vt,xt,Wt,Ot,Pt,qt,Zt,zt,Kt,Xt,_t,jt,$t,Ar,er,tr,rr,nr,sr,or,ir,ar,gr,cr,Qr,Er,Br,hr,Ir,Cr,lr,ur,dr,fr,pr,wr,yr,Dr,mr,kr,Rr,br,Fr,Sr,Nr,Mr,Ur,Lr,Tr,Gr,Yr,vr,Jr,Hr,Vr,xr,Wr,Or,Pr,qr,Zr,zr,Kr,Xr,_r,jr,$r,An,en,tn,rn,nn,sn,on={};function an(){if(mt)return on;mt=1;const{getResponseData:A,buildKey:e,addMockDispatch:t}=Dt(),{kDispatches:r,kDispatchKey:n,kDefaultHeaders:s,kDefaultTrailers:o,kContentLength:i,kMockDispatch:a}=yt(),{InvalidArgumentError:g}=hA(),{buildURL:c}=CA();
/**
	 * Defines the scope API for an interceptor reply
	 */
class Q{constructor(A){this[a]=A}
/**
	   * Delay a reply by a set amount in ms.
	   */delay(A){if("number"!=typeof A||!Number.isInteger(A)||A<=0)throw new g("waitInMs must be a valid integer > 0");return this[a].delay=A,this}
/**
	   * For a defined reply, never mark as consumed.
	   */persist(){return this[a].persist=!0,this}
/**
	   * Allow one to define a reply for a set amount of matching requests.
	   */times(A){if("number"!=typeof A||!Number.isInteger(A)||A<=0)throw new g("repeatTimes must be a valid integer > 0");return this[a].times=A,this}}
/**
	 * Defines an interceptor for a Mock
	 */return on.MockInterceptor=class{constructor(A,t){if("object"!=typeof A)throw new g("opts must be an object");if(void 0===A.path)throw new g("opts.path must be defined");
// See https://github.com/nodejs/undici/issues/1245
// As per RFC 3986, clients are not supposed to send URI
// fragments to servers when they retrieve a document,
if(void 0===A.method&&(A.method="GET"),"string"==typeof A.path)if(A.query)A.path=c(A.path,A.query);else{
// Matches https://github.com/nodejs/undici/blob/main/lib/web/fetch/index.js#L1811
const e=new URL(A.path,"data://");A.path=e.pathname+e.search}"string"==typeof A.method&&(A.method=A.method.toUpperCase()),this[n]=e(A),this[r]=t,this[s]={},this[o]={},this[i]=!1}createMockScopeDispatchData({statusCode:e,data:t,responseOptions:r}){const n=A(t),a=this[i]?{"content-length":n.length}:{};return{statusCode:e,data:t,headers:{...this[s],...a,...r.headers},trailers:{...this[o],...r.trailers}}}validateReplyParameters(A){if(void 0===A.statusCode)throw new g("statusCode must be defined");if("object"!=typeof A.responseOptions||null===A.responseOptions)throw new g("responseOptions must be an object")}
/**
	   * Mock an undici request with a defined reply.
	   */reply(A){
// Values of reply aren't available right now as they
// can only be available when the reply callback is invoked.
if("function"==typeof A){
// We'll first wrap the provided callback in another function,
// this function will properly resolve the data from the callback
// when invoked.
const e=e=>{
// Our reply options callback contains the parameter for statusCode, data and options.
const t=A(e);
// Check if it is in the right format
if("object"!=typeof t||null===t)throw new g("reply options callback must return an object");const r={data:"",responseOptions:{},...t};
// Since the values can be obtained immediately we return them
// from this higher order function that will be resolved later.
return this.validateReplyParameters(r),{...this.createMockScopeDispatchData(r)}},s=t(this[r],this[n],e);
// Add usual dispatch data, but this time set the data parameter to function that will eventually provide data.
return new Q(s)}
// We can have either one or three parameters, if we get here,
// we should have 1-3 parameters. So we spread the arguments of
// this function to obtain the parameters, since replyData will always
// just be the statusCode.
const e={statusCode:A,data:void 0===arguments[1]?"":arguments[1],responseOptions:void 0===arguments[2]?{}:arguments[2]};this.validateReplyParameters(e);
// Send in-already provided data like usual
const s=this.createMockScopeDispatchData(e),o=t(this[r],this[n],s);return new Q(o)}
/**
	   * Mock an undici request with a defined error.
	   */replyWithError(A){if(void 0===A)throw new g("error must be defined");const e=t(this[r],this[n],{error:A});return new Q(e)}
/**
	   * Set default reply headers on the interceptor for subsequent replies
	   */defaultReplyHeaders(A){if(void 0===A)throw new g("headers must be defined");return this[s]=A,this}
/**
	   * Set default reply trailers on the interceptor for subsequent replies
	   */defaultReplyTrailers(A){if(void 0===A)throw new g("trailers must be defined");return this[o]=A,this}
/**
	   * Set reply content length header for replies on the interceptor
	   */replyContentLength(){return this[i]=!0,this}},on.MockScope=Q,on}function gn(){if(Rt)return kt;Rt=1;const{promisify:A}=l,e=Ve(),{buildMockDispatch:t}=Dt(),{kDispatches:r,kMockAgent:n,kClose:s,kOriginalClose:o,kOrigin:i,kOriginalDispatch:a,kConnected:g}=yt(),{MockInterceptor:c}=an(),Q=BA(),{InvalidArgumentError:E}=hA();
/**
	 * MockClient provides an API that extends the Client to influence the mockDispatches.
	 */
class B extends e{constructor(A,e){if(super(A,e),!e||!e.agent||"function"!=typeof e.agent.dispatch)throw new E("Argument opts.agent must implement Agent");this[n]=e.agent,this[i]=A,this[r]=[],this[g]=1,this[a]=this.dispatch,this[o]=this.close.bind(this),this.dispatch=t.call(this),this.close=this[s]}get[Q.kConnected](){return this[g]}
/**
	   * Sets up the base interceptor for mocking replies from undici.
	   */intercept(A){return new c(A,this[r])}async[s](){await A(this[o])(),this[g]=0,this[n][Q.kClients].delete(this[i])}}return kt=B}function cn(){if(Ft)return bt;Ft=1;const{promisify:A}=l,e=Oe(),{buildMockDispatch:t}=Dt(),{kDispatches:r,kMockAgent:n,kClose:s,kOriginalClose:o,kOrigin:i,kOriginalDispatch:a,kConnected:g}=yt(),{MockInterceptor:c}=an(),Q=BA(),{InvalidArgumentError:E}=hA();
/**
	 * MockPool provides an API that extends the Pool to influence the mockDispatches.
	 */
class B extends e{constructor(A,e){if(super(A,e),!e||!e.agent||"function"!=typeof e.agent.dispatch)throw new E("Argument opts.agent must implement Agent");this[n]=e.agent,this[i]=A,this[r]=[],this[g]=1,this[a]=this.dispatch,this[o]=this.close.bind(this),this.dispatch=t.call(this),this.close=this[s]}get[Q.kConnected](){return this[g]}
/**
	   * Sets up the base interceptor for mocking replies from undici.
	   */intercept(A){return new c(A,this[r])}async[s](){await A(this[o])(),this[g]=0,this[n][Q.kClients].delete(this[i])}}return bt=B}function Qn(){if(Tt)return Lt;Tt=1;const{kClients:A}=BA(),e=Pe(),{kAgent:t,kMockAgentSet:r,kMockAgentGet:n,kDispatches:s,kIsMockActive:o,kNetConnect:i,kGetNetConnect:a,kOptions:g,kFactory:c}=yt(),Q=gn(),E=cn(),{matchValue:B,buildMockOptions:h}=Dt(),{InvalidArgumentError:C,UndiciError:l}=hA(),u=uA(),d=function(){if(Nt)return St;Nt=1;const A={pronoun:"it",is:"is",was:"was",this:"this"},e={pronoun:"they",is:"are",was:"were",this:"these"};return St=class{constructor(A,e){this.singular=A,this.plural=e}pluralize(t){const r=1===t;return{...r?A:e,count:t,noun:r?this.singular:this.plural}}}}(),f=function(){if(Ut)return Mt;Ut=1;const{Transform:A}=I,{Console:e}=b,t=process.versions.icu?"✅":"Y ",r=process.versions.icu?"❌":"N ";
/**
	 * Gets the output of `console.table(…)` as a string.
	 */
return Mt=class{constructor({disableColors:t}={}){this.transform=new A({transform(A,e,t){t(null,A)}}),this.logger=new e({stdout:this.transform,inspectOptions:{colors:!t&&!process.env.CI}})}format(A){const e=A.map(({method:A,path:e,data:{statusCode:n},persist:s,times:o,timesInvoked:i,origin:a})=>({Method:A,Origin:a,Path:e,"Status code":n,Persistent:s?t:r,Invocations:i,Remaining:s?1/0:o-i}));return this.logger.table(e),this.transform.read().toString()}},Mt}();return Lt=class extends u{constructor(r){
// Instantiate Agent and encapsulate
if(super(r),this[i]=!0,this[o]=!0,r?.agent&&"function"!=typeof r.agent.dispatch)throw new C("Argument opts.agent must implement Agent");const n=r?.agent?r.agent:new e(r);this[t]=n,this[A]=n[A],this[g]=h(r)}get(A){let e=this[n](A);return e||(e=this[c](A),this[r](A,e)),e}dispatch(A,e){
// Call MockAgent.get to perform additional setup before dispatching as normal
return this.get(A.origin),this[t].dispatch(A,e)}async close(){await this[t].close(),this[A].clear()}deactivate(){this[o]=!1}activate(){this[o]=!0}enableNetConnect(A){if("string"==typeof A||"function"==typeof A||A instanceof RegExp)Array.isArray(this[i])?this[i].push(A):this[i]=[A];else{if(void 0!==A)throw new C("Unsupported matcher. Must be one of String|Function|RegExp.");this[i]=!0}}disableNetConnect(){this[i]=!1}
// This is required to bypass issues caused by using global symbols - see:
// https://github.com/nodejs/undici/issues/1447
get isMockActive(){return this[o]}[r](e,t){this[A].set(e,t)}[c](A){const e=Object.assign({agent:this},this[g]);return this[g]&&1===this[g].connections?new Q(A,e):new E(A,e)}[n](e){
// First check if we can immediately find it
const t=this[A].get(e);if(t)return t;
// If the origin is not a string create a dummy parent pool and return to user
if("string"!=typeof e){const A=this[c]("http://localhost:9999");return this[r](e,A),A}
// If we match, create a pool and assign the same dispatches
for(const[t,n]of Array.from(this[A]))if(n&&"string"!=typeof t&&B(t,e)){const A=this[c](e);return this[r](e,A),A[s]=n[s],A}}[a](){return this[i]}pendingInterceptors(){const e=this[A];return Array.from(e.entries()).flatMap(([A,e])=>e[s].map(e=>({...e,origin:A}))).filter(({pending:A})=>A)}assertNoPendingInterceptors({pendingInterceptorsFormatter:A=new f}={}){const e=this.pendingInterceptors();if(0===e.length)return;const t=new d("interceptor","interceptors").pluralize(e.length);throw new l(`\n${t.count} ${t.noun} ${t.is} pending:\n\n${A.format(e)}\n`.trim())}},Lt}function En(){if(Yt)return Gt;Yt=1;
// We include a version number for the Dispatcher API. In case of breaking changes,
// this version number must be increased to avoid conflicts.
const A=Symbol.for("undici.globalDispatcher.1"),{InvalidArgumentError:e}=hA(),t=Pe();function r(t){if(!t||"function"!=typeof t.dispatch)throw new e("Argument agent must implement Agent");Object.defineProperty(globalThis,A,{value:t,writable:!0,enumerable:!1,configurable:!1})}function n(){return globalThis[A]}return void 0===n()&&r(new t),Gt={setGlobalDispatcher:r,getGlobalDispatcher:n}}function Bn(){return Jt?vt:(Jt=1,vt=class{#c;constructor(A){if("object"!=typeof A||null===A)throw new TypeError("handler must be an object");this.#c=A}onConnect(...A){return this.#c.onConnect?.(...A)}onError(...A){return this.#c.onError?.(...A)}onUpgrade(...A){return this.#c.onUpgrade?.(...A)}onResponseStarted(...A){return this.#c.onResponseStarted?.(...A)}onHeaders(...A){return this.#c.onHeaders?.(...A)}onData(...A){return this.#c.onData?.(...A)}onComplete(...A){return this.#c.onComplete?.(...A)}onBodySent(...A){return this.#c.onBodySent?.(...A)}})}function hn(){if(Vt)return Ht;Vt=1;const A=Je();return Ht=e=>{const t=e?.maxRedirections;return e=>function(r,n){const{maxRedirections:s=t,...o}=r;if(!s)return e(r,n);const i=new A(e,s,r,n);return e(o,i)}},Ht}function In(){if(Wt)return xt;Wt=1;const A=Ze();return xt=e=>t=>function(r,n){return t(r,new A({...r,retryOptions:{...e,...r.retryOptions}},{handler:n,dispatch:t}))}}function Cn(){if(Pt)return Ot;Pt=1;const A=CA(),{InvalidArgumentError:e,RequestAbortedError:t}=hA(),r=Bn();class n extends r{#Q=1048576;#E=null;#B=!1;#h=!1;#I=0;#C=null;#c=null;constructor({maxSize:A},t){if(super(t),null!=A&&(!Number.isFinite(A)||A<1))throw new e("maxSize must be a number greater than 0");this.#Q=A??this.#Q,this.#c=t}onConnect(A){this.#E=A,this.#c.onConnect(this.#l.bind(this))}#l(A){this.#h=!0,this.#C=A}
// TODO: will require adjustment after new hooks are out
onHeaders(e,r,n,s){const o=A.parseHeaders(r)["content-length"];if(null!=o&&o>this.#Q)throw new t(`Response size (${o}) larger than maxSize (${this.#Q})`);return!!this.#h||this.#c.onHeaders(e,r,n,s)}onError(A){this.#B||(A=this.#C??A,this.#c.onError(A))}onData(A){return this.#I=this.#I+A.length,this.#I>=this.#Q&&(this.#B=!0,this.#h?this.#c.onError(this.#C):this.#c.onComplete([])),!0}onComplete(A){this.#B||(this.#h?this.#c.onError(this.reason):this.#c.onComplete(A))}}return Ot=function({maxSize:A}={maxSize:1048576}){return e=>function(t,r){const{dumpMaxSize:s=A}=t,o=new n({maxSize:s},r);return e(t,o)}}}function ln(){if(Zt)return qt;Zt=1;const{isIP:A}=B,{lookup:e}=F,t=Bn(),{InvalidArgumentError:r,InformationalError:n}=hA(),s=Math.pow(2,31)-1;class o{#u=0;#d=0;#f=new Map;dualStack=!0;affinity=null;lookup=null;pick=null;constructor(A){this.#u=A.maxTTL,this.#d=A.maxItems,this.dualStack=A.dualStack,this.affinity=A.affinity,this.lookup=A.lookup??this.#p,this.pick=A.pick??this.#w}get full(){return this.#f.size===this.#d}runLookup(A,e,t){const r=this.#f.get(A.hostname);
// If full, we just return the origin
if(null==r&&this.full)return void t(null,A.origin);const s={affinity:this.affinity,dualStack:this.dualStack,lookup:this.lookup,pick:this.pick,...e.dns,maxTTL:this.#u,maxItems:this.#d};
// If no IPs we lookup
if(null==r)this.lookup(A,s,(e,r)=>{if(e||null==r||0===r.length)return void t(e??new n("No DNS entries found"));this.setRecords(A,r);const o=this.#f.get(A.hostname),i=this.pick(A,o,s.affinity);let a;a="number"==typeof i.port?`:${i.port}`:""!==A.port?`:${A.port}`:"",t(null,`${A.protocol}//${6===i.family?`[${i.address}]`:i.address}${a}`)});else{
// If there's IPs we pick
const n=this.pick(A,r,s.affinity);
// If no IPs we lookup - deleting old records
if(null==n)return this.#f.delete(A.hostname),void this.runLookup(A,e,t);let o;o="number"==typeof n.port?`:${n.port}`:""!==A.port?`:${A.port}`:"",t(null,`${A.protocol}//${6===n.family?`[${n.address}]`:n.address}${o}`)}}#p(A,t,r){e(A.hostname,{all:!0,family:!1===this.dualStack?this.affinity:0,order:"ipv4first"},(A,e)=>{if(A)return r(A);const t=new Map;for(const A of e)
// On linux we found duplicates, we attempt to remove them with
// the latest record
t.set(`${A.address}:${A.family}`,A);r(null,t.values())})}#w(A,e,t){let r=null;const{records:n,offset:o}=e;let i;
// If no IPs we return null
if(this.dualStack?(null==t&&(
// Balance between ip families
null==o||o===s?(e.offset=0,t=4):(e.offset++,t=1&~e.offset?4:6)),i=null!=n[t]&&n[t].ips.length>0?n[t]:n[4===t?6:4]):i=n[t],null==i||0===i.ips.length)return r;null==i.offset||i.offset===s?i.offset=0:i.offset++;const a=i.offset%i.ips.length;return r=i.ips[a]??null,null==r?r:Date.now()-r.timestamp>r.ttl?(// record TTL is already in ms
// We delete expired records
// It is possible that they have different TTL, so we manage them individually
i.ips.splice(a,1),this.pick(A,e,t)):r}setRecords(A,e){const t=Date.now(),r={records:{4:null,6:null}};for(const A of e){A.timestamp=t,"number"==typeof A.ttl?
// The record TTL is expected to be in ms
A.ttl=Math.min(A.ttl,this.#u):A.ttl=this.#u;const e=r.records[A.family]??{ips:[]};e.ips.push(A),r.records[A.family]=e}this.#f.set(A.hostname,r)}getHandler(A,e){return new i(this,A,e)}}class i extends t{#y=null;#D=null;#e=null;#c=null;#m=null;constructor(A,{origin:e,handler:t,dispatch:r},n){super(t),this.#m=e,this.#c=t,this.#D={...n},this.#y=A,this.#e=r}onError(A){switch(A.code){case"ETIMEDOUT":case"ECONNREFUSED":return this.#y.dualStack?
// We delete the record and retry
void this.#y.runLookup(this.#m,this.#D,(A,e)=>{if(A)return this.#c.onError(A);const t={...this.#D,origin:e};this.#e(t,this)}):void this.#c.onError(A);case"ENOTFOUND":this.#y.deleteRecord(this.#m);
// eslint-disable-next-line no-fallthrough
default:this.#c.onError(A)}}}return qt=e=>{if(null!=e?.maxTTL&&("number"!=typeof e?.maxTTL||e?.maxTTL<0))throw new r("Invalid maxTTL. Must be a positive number");if(null!=e?.maxItems&&("number"!=typeof e?.maxItems||e?.maxItems<1))throw new r("Invalid maxItems. Must be a positive number and greater than zero");if(null!=e?.affinity&&4!==e?.affinity&&6!==e?.affinity)throw new r("Invalid affinity. Must be either 4 or 6");if(null!=e?.dualStack&&"boolean"!=typeof e?.dualStack)throw new r("Invalid dualStack. Must be a boolean");if(null!=e?.lookup&&"function"!=typeof e?.lookup)throw new r("Invalid lookup. Must be a function");if(null!=e?.pick&&"function"!=typeof e?.pick)throw new r("Invalid pick. Must be a function");const t=e?.dualStack??!0;let n;n=t?e?.affinity??null:e?.affinity??4;const s=new o({maxTTL:e?.maxTTL??1e4,// Expressed in ms
lookup:e?.lookup??null,pick:e?.pick??null,dualStack:t,affinity:n,maxItems:e?.maxItems??1/0});return e=>function(t,r){const n=t.origin.constructor===URL?t.origin:new URL(t.origin);return 0!==A(n.hostname)?e(t,r):(s.runLookup(n,t,(A,o)=>{if(A)return r.onError(A);let i=null;i={...t,servername:n.hostname,// For SNI on TLS
origin:o,headers:{host:n.hostname,...t.headers}},e(i,s.getHandler({origin:n,dispatch:e,handler:r},t))}),!0)}}}function un(){if(Kt)return zt;Kt=1;const{kConstruct:A}=BA(),{kEnumerableProperty:e}=CA(),{iteratorMixin:t,isValidHeaderName:r,isValidHeaderValue:n}=Me(),{webidl:s}=Ne(),o=E,i=l,a=Symbol("headers map"),g=Symbol("headers map sorted");
/**
	 * @param {number} code
	 */
function c(A){return 10===A||13===A||9===A||32===A}
/**
	 * @see https://fetch.spec.whatwg.org/#concept-header-value-normalize
	 * @param {string} potentialValue
	 */function Q(A){
//  To normalize a byte sequence potentialValue, remove
//  any leading and trailing HTTP whitespace bytes from
//  potentialValue.
let e=0,t=A.length;for(;t>e&&c(A.charCodeAt(t-1));)--t;for(;t>e&&c(A.charCodeAt(e));)++e;return 0===e&&t===A.length?A:A.substring(e,t)}function B(A,e){
// To fill a Headers object headers with a given object object, run these steps:
// 1. If object is a sequence, then for each header in object:
// Note: webidl conversion to array has already been done.
if(Array.isArray(e))for(let t=0;t<e.length;++t){const r=e[t];
// 1. If header does not contain exactly two items, then throw a TypeError.
if(2!==r.length)throw s.errors.exception({header:"Headers constructor",message:`expected name/value pair to be length 2, found ${r.length}.`});
// 2. Append (header’s first item, header’s second item) to headers.
h(A,r[0],r[1])}else{if("object"!=typeof e||null===e)throw s.errors.conversionFailed({prefix:"Headers constructor",argument:"Argument 1",types:["sequence<sequence<ByteString>>","record<ByteString, ByteString>"]});{
// Note: null should throw
// 2. Otherwise, object is a record, then for each key → value in object,
//    append (key, value) to headers
const t=Object.keys(e);for(let r=0;r<t.length;++r)h(A,t[r],e[t[r]])}}}
/**
	 * @see https://fetch.spec.whatwg.org/#concept-headers-append
	 */function h(A,e,t){
// 2. If name is not a header name or value is not a
//    header value, then throw a TypeError.
if(
// 1. Normalize value.
t=Q(t),!r(e))throw s.errors.invalidArgument({prefix:"Headers.append",value:e,type:"header name"});
// 3. If headers’s guard is "immutable", then throw a TypeError.
// 4. Otherwise, if headers’s guard is "request" and name is a
//    forbidden header name, return.
// 5. Otherwise, if headers’s guard is "request-no-cors":
//    TODO
// Note: undici does not implement forbidden header names
if(!n(t))throw s.errors.invalidArgument({prefix:"Headers.append",value:t,type:"header value"});if("immutable"===d(A))throw new TypeError("immutable");
// 6. Otherwise, if headers’s guard is "response" and name is a
//    forbidden response-header name, return.
// 7. Append (name, value) to headers’s header list.
return p(A).append(e,t,!1);
// 8. If headers’s guard is "request-no-cors", then remove
//    privileged no-CORS request headers from headers
}function I(A,e){return A[0]<e[0]?-1:1}class C{
/** @type {[string, string][]|null} */
cookies=null;constructor(A){A instanceof C?(this[a]=new Map(A[a]),this[g]=A[g],this.cookies=null===A.cookies?null:[...A.cookies]):(this[a]=new Map(A),this[g]=null)}
/**
	   * @see https://fetch.spec.whatwg.org/#header-list-contains
	   * @param {string} name
	   * @param {boolean} isLowerCase
	   */contains(A,e){
// A header list list contains a header name name if list
// contains a header whose name is a byte-case-insensitive
// match for name.
return this[a].has(e?A:A.toLowerCase())}clear(){this[a].clear(),this[g]=null,this.cookies=null}
/**
	   * @see https://fetch.spec.whatwg.org/#concept-header-list-append
	   * @param {string} name
	   * @param {string} value
	   * @param {boolean} isLowerCase
	   */append(A,e,t){this[g]=null;
// 1. If list contains name, then set name to the first such
//    header’s name.
const r=t?A:A.toLowerCase(),n=this[a].get(r);
// 2. Append (name, value) to list.
if(n){const A="cookie"===r?"; ":", ";this[a].set(r,{name:n.name,value:`${n.value}${A}${e}`})}else this[a].set(r,{name:A,value:e});"set-cookie"===r&&(this.cookies??=[]).push(e)}
/**
	   * @see https://fetch.spec.whatwg.org/#concept-header-list-set
	   * @param {string} name
	   * @param {string} value
	   * @param {boolean} isLowerCase
	   */set(A,e,t){this[g]=null;const r=t?A:A.toLowerCase();"set-cookie"===r&&(this.cookies=[e]),
// 1. If list contains name, then set the value of
//    the first such header to value and remove the
//    others.
// 2. Otherwise, append header (name, value) to list.
this[a].set(r,{name:A,value:e})}
/**
	   * @see https://fetch.spec.whatwg.org/#concept-header-list-delete
	   * @param {string} name
	   * @param {boolean} isLowerCase
	   */delete(A,e){this[g]=null,e||(A=A.toLowerCase()),"set-cookie"===A&&(this.cookies=null),this[a].delete(A)}
/**
	   * @see https://fetch.spec.whatwg.org/#concept-header-list-get
	   * @param {string} name
	   * @param {boolean} isLowerCase
	   * @returns {string | null}
	   */get(A,e){
// 1. If list does not contain name, then return null.
// 2. Return the values of all headers in list whose name
//    is a byte-case-insensitive match for name,
//    separated from each other by 0x2C 0x20, in order.
return this[a].get(e?A:A.toLowerCase())?.value??null}*[Symbol.iterator](){
// use the lowercased name
for(const{0:A,1:{value:e}}of this[a])yield[A,e]}get entries(){const A={};if(0!==this[a].size)for(const{name:e,value:t}of this[a].values())A[e]=t;return A}rawValues(){return this[a].values()}get entriesList(){const A=[];if(0!==this[a].size)for(const{0:e,1:{name:t,value:r}}of this[a])if("set-cookie"===e)for(const e of this.cookies)A.push([t,e]);else A.push([t,r]);return A}
// https://fetch.spec.whatwg.org/#convert-header-names-to-a-sorted-lowercase-set
toSortedArray(){const A=this[a].size,e=new Array(A);
// In most cases, you will use the fast-path.
// fast-path: Use binary insertion sort for small arrays.
if(A<=32){if(0===A)
// If empty, it is an empty array. To avoid the first index assignment.
return e;
// Improve performance by unrolling loop and avoiding double-loop.
// Double-loop-less version of the binary insertion sort.
const t=this[a][Symbol.iterator](),r=t.next().value;
// set [name, value] to first index.
e[0]=[r[0],r[1].value],
// https://fetch.spec.whatwg.org/#concept-header-list-sort-and-combine
// 3.2.2. Assert: value is non-null.
o(null!==r[1].value);for(let r,n,s=1,i=0,a=0,g=0,c=0;s<A;++s){
// binary search
for(
// get next value
n=t.next().value,
// set [name, value] to current index.
r=e[s]=[n[0],n[1].value],
// https://fetch.spec.whatwg.org/#concept-header-list-sort-and-combine
// 3.2.2. Assert: value is non-null.
o(null!==r[1]),g=0,a=s;g<a;)
// middle index
c=g+(a-g>>1),
// compare header name
e[c][0]<=r[0]?g=c+1:a=c;if(s!==c){for(i=s;i>g;)e[i]=e[--i];e[g]=r}}
/* c8 ignore next 4 */if(!t.next().done)
// This is for debugging and will never be called.
throw new TypeError("Unreachable");return e}{
// This case would be a rare occurrence.
// slow-path: fallback
let A=0;for(const{0:t,1:{value:r}}of this[a])e[A++]=[t,r],
// https://fetch.spec.whatwg.org/#concept-header-list-sort-and-combine
// 3.2.2. Assert: value is non-null.
o(null!==r);return e.sort(I)}}}
// https://fetch.spec.whatwg.org/#headers-class
class u{#k;#R;constructor(e=void 0){s.util.markAsUncloneable(this),e!==A&&(this.#R=new C,
// The new Headers(init) constructor steps are:
// 1. Set this’s guard to "none".
this.#k="none",
// 2. If init is given, then fill this with init.
void 0!==e&&B(this,e=s.converters.HeadersInit(e,"Headers contructor","init")))}
// https://fetch.spec.whatwg.org/#dom-headers-append
append(A,e){s.brandCheck(this,u),s.argumentLengthCheck(arguments,2,"Headers.append");const t="Headers.append";return h(this,A=s.converters.ByteString(A,t,"name"),e=s.converters.ByteString(e,t,"value"))}
// https://fetch.spec.whatwg.org/#dom-headers-delete
delete(A){s.brandCheck(this,u),s.argumentLengthCheck(arguments,1,"Headers.delete");
// 1. If name is not a header name, then throw a TypeError.
if(A=s.converters.ByteString(A,"Headers.delete","name"),!r(A))throw s.errors.invalidArgument({prefix:"Headers.delete",value:A,type:"header name"});
// 2. If this’s guard is "immutable", then throw a TypeError.
// 3. Otherwise, if this’s guard is "request" and name is a
//    forbidden header name, return.
// 4. Otherwise, if this’s guard is "request-no-cors", name
//    is not a no-CORS-safelisted request-header name, and
//    name is not a privileged no-CORS request-header name,
//    return.
// 5. Otherwise, if this’s guard is "response" and name is
//    a forbidden response-header name, return.
// Note: undici does not implement forbidden header names
if("immutable"===this.#k)throw new TypeError("immutable");
// 6. If this’s header list does not contain name, then
//    return.
this.#R.contains(A,!1)&&
// 7. Delete name from this’s header list.
// 8. If this’s guard is "request-no-cors", then remove
//    privileged no-CORS request headers from this.
this.#R.delete(A,!1)}
// https://fetch.spec.whatwg.org/#dom-headers-get
get(A){s.brandCheck(this,u),s.argumentLengthCheck(arguments,1,"Headers.get");const e="Headers.get";
// 1. If name is not a header name, then throw a TypeError.
if(A=s.converters.ByteString(A,e,"name"),!r(A))throw s.errors.invalidArgument({prefix:e,value:A,type:"header name"});
// 2. Return the result of getting name from this’s header
//    list.
return this.#R.get(A,!1)}
// https://fetch.spec.whatwg.org/#dom-headers-has
has(A){s.brandCheck(this,u),s.argumentLengthCheck(arguments,1,"Headers.has");const e="Headers.has";
// 1. If name is not a header name, then throw a TypeError.
if(A=s.converters.ByteString(A,e,"name"),!r(A))throw s.errors.invalidArgument({prefix:e,value:A,type:"header name"});
// 2. Return true if this’s header list contains name;
//    otherwise false.
return this.#R.contains(A,!1)}
// https://fetch.spec.whatwg.org/#dom-headers-set
set(A,e){s.brandCheck(this,u),s.argumentLengthCheck(arguments,2,"Headers.set");const t="Headers.set";
// 2. If name is not a header name or value is not a
//    header value, then throw a TypeError.
if(A=s.converters.ByteString(A,t,"name"),
// 1. Normalize value.
e=Q(e=s.converters.ByteString(e,t,"value")),!r(A))throw s.errors.invalidArgument({prefix:t,value:A,type:"header name"});
// 3. If this’s guard is "immutable", then throw a TypeError.
// 4. Otherwise, if this’s guard is "request" and name is a
//    forbidden header name, return.
// 5. Otherwise, if this’s guard is "request-no-cors" and
//    name/value is not a no-CORS-safelisted request-header,
//    return.
// 6. Otherwise, if this’s guard is "response" and name is a
//    forbidden response-header name, return.
// Note: undici does not implement forbidden header names
if(!n(e))throw s.errors.invalidArgument({prefix:t,value:e,type:"header value"});if("immutable"===this.#k)throw new TypeError("immutable");
// 7. Set (name, value) in this’s header list.
// 8. If this’s guard is "request-no-cors", then remove
//    privileged no-CORS request headers from this
this.#R.set(A,e,!1)}
// https://fetch.spec.whatwg.org/#dom-headers-getsetcookie
getSetCookie(){s.brandCheck(this,u);
// 1. If this’s header list does not contain `Set-Cookie`, then return « ».
// 2. Return the values of all headers in this’s header list whose name is
//    a byte-case-insensitive match for `Set-Cookie`, in order.
const A=this.#R.cookies;return A?[...A]:[]}
// https://fetch.spec.whatwg.org/#concept-header-list-sort-and-combine
get[g](){if(this.#R[g])return this.#R[g];
// 1. Let headers be an empty list of headers with the key being the name
//    and value the value.
const A=[],e=this.#R.toSortedArray(),t=this.#R.cookies;
// 2. Let names be the result of convert header names to a sorted-lowercase
//    set with all the names of the headers in list.
// fast-path
if(null===t||1===t.length)
// Note: The non-null assertion of value has already been done by `HeadersList#toSortedArray`
return this.#R[g]=e;
// 3. For each name of names:
for(let r=0;r<e.length;++r){const{0:n,1:s}=e[r];
// 1. If name is `set-cookie`, then:
if("set-cookie"===n)
// 1. Let values be a list of all values of headers in list whose name
//    is a byte-case-insensitive match for name, in order.
// 2. For each value of values:
// 1. Append (name, value) to headers.
for(let e=0;e<t.length;++e)A.push([n,t[e]]);else
// 2. Otherwise:
// 1. Let value be the result of getting name from list.
// 2. Assert: value is non-null.
// Note: This operation was done by `HeadersList#toSortedArray`.
// 3. Append (name, value) to headers.
A.push([n,s])}
// 4. Return headers.
return this.#R[g]=A}[i.inspect.custom](A,e){return e.depth??=A,`Headers ${i.formatWithOptions(e,this.#R.entries)}`}static getHeadersGuard(A){return A.#k}static setHeadersGuard(A,e){A.#k=e}static getHeadersList(A){return A.#R}static setHeadersList(A,e){A.#R=e}}const{getHeadersGuard:d,setHeadersGuard:f,getHeadersList:p,setHeadersList:w}=u;return Reflect.deleteProperty(u,"getHeadersGuard"),Reflect.deleteProperty(u,"setHeadersGuard"),Reflect.deleteProperty(u,"getHeadersList"),Reflect.deleteProperty(u,"setHeadersList"),t("Headers",u,g,0,1),Object.defineProperties(u.prototype,{append:e,delete:e,get:e,has:e,set:e,getSetCookie:e,[Symbol.toStringTag]:{value:"Headers",configurable:!0},[i.inspect.custom]:{enumerable:!1}}),s.converters.HeadersInit=function(A,e,t){if("Object"===s.util.Type(A)){const r=Reflect.get(A,Symbol.iterator);
// A work-around to ensure we send the properly-cased Headers when V is a Headers object.
// Read https://github.com/nodejs/undici/pull/3159#issuecomment-2075537226 before touching, please.
if(!i.types.isProxy(A)&&r===u.prototype.entries)// Headers object
try{return p(A).entriesList}catch{
// fall-through
}return"function"==typeof r?s.converters["sequence<sequence<ByteString>>"](A,e,t,r.bind(A)):s.converters["record<ByteString, ByteString>"](A,e,t)}throw s.errors.conversionFailed({prefix:"Headers constructor",argument:"Argument 1",types:["sequence<sequence<ByteString>>","record<ByteString, ByteString>"]})},zt={fill:B,
// for test.
compareHeaderName:I,Headers:u,HeadersList:C,getHeadersGuard:d,setHeadersGuard:f,setHeadersList:w,getHeadersList:p}}function dn(){if(_t)return Xt;_t=1;const{Headers:A,HeadersList:e,fill:t,getHeadersGuard:r,setHeadersGuard:n,setHeadersList:s}=un(),{extractBody:o,cloneBody:i,mixinBody:a,hasFinalizationRegistry:g,streamRegistry:c,bodyUnusable:Q}=Ge(),B=CA(),h=l,{kEnumerableProperty:I}=B,{isValidReasonPhrase:C,isCancelled:u,isAborted:d,isBlobLike:f,serializeJavascriptValueToJSONString:p,isErrorLike:w,isomorphicEncode:y,environmentSettingsObject:D}=Me(),{redirectStatusSet:m,nullBodyStatus:k}=be(),{kState:R,kHeaders:b}=Ue(),{webidl:F}=Ne(),{FormData:S}=Te(),{URLSerializer:N}=Se(),{kConstruct:M}=BA(),U=E,{types:L}=l,T=new TextEncoder("utf-8");
// https://fetch.spec.whatwg.org/#response-class
class G{
// Creates network error Response.
static error(){return W(J(),"immutable")}
// https://fetch.spec.whatwg.org/#dom-response-json
static json(A,e={}){F.argumentLengthCheck(arguments,1,"Response.json"),null!==e&&(e=F.converters.ResponseInit(e));
// 1. Let bytes the result of running serialize a JavaScript value to JSON bytes on data.
const t=T.encode(p(A)),r=o(t),n=W(v({}),"response");
// 2. Let body be the result of extracting bytes.
// 5. Return responseObject.
// 4. Perform initialize a response given responseObject, init, and (body, "application/json").
return x(n,e,{body:r[0],type:"application/json"}),n}
// Creates a redirect Response that redirects to url with status status.
static redirect(A,e=302){
// 1. Let parsedURL be the result of parsing url with current settings
// object’s API base URL.
// 2. If parsedURL is failure, then throw a TypeError.
// TODO: base-URL?
let t;F.argumentLengthCheck(arguments,1,"Response.redirect"),A=F.converters.USVString(A),e=F.converters["unsigned short"](e);try{t=new URL(A,D.settingsObject.baseUrl)}catch(e){throw new TypeError(`Failed to parse URL from ${A}`,{cause:e})}
// 3. If status is not a redirect status, then throw a RangeError.
if(!m.has(e))throw new RangeError(`Invalid status code ${e}`);
// 4. Let responseObject be the result of creating a Response object,
// given a new response, "immutable", and this’s relevant Realm.
const r=W(v({}),"immutable");
// 5. Set responseObject’s response’s status to status.
r[R].status=e;
// 6. Let value be parsedURL, serialized and isomorphic encoded.
const n=y(N(t));
// 7. Append `Location`/value to responseObject’s response’s header list.
// 8. Return responseObject.
return r[R].headersList.append("location",n,!0),r}
// https://fetch.spec.whatwg.org/#dom-response
constructor(e=null,t={}){if(F.util.markAsUncloneable(this),e===M)return;null!==e&&(e=F.converters.BodyInit(e)),t=F.converters.ResponseInit(t),
// 1. Set this’s response to a new response.
this[R]=v({}),
// 2. Set this’s headers to a new Headers object with this’s relevant
// Realm, whose header list is this’s response’s header list and guard
// is "response".
this[b]=new A(M),n(this[b],"response"),s(this[b],this[R].headersList);
// 3. Let bodyWithType be null.
let r=null;
// 4. If body is non-null, then set bodyWithType to the result of extracting body.
if(null!=e){const[A,t]=o(e);r={body:A,type:t}}
// 5. Perform initialize a response given this, init, and bodyWithType.
x(this,t,r)}
// Returns response’s type, e.g., "cors".
get type(){
// The type getter steps are to return this’s response’s type.
return F.brandCheck(this,G),this[R].type}
// Returns response’s URL, if it has one; otherwise the empty string.
get url(){F.brandCheck(this,G);const A=this[R].urlList,e=A[A.length-1]??null;
// The url getter steps are to return the empty string if this’s
// response’s URL is null; otherwise this’s response’s URL,
// serialized with exclude fragment set to true.
return null===e?"":N(e,!0)}
// Returns whether response was obtained through a redirect.
get redirected(){
// The redirected getter steps are to return true if this’s response’s URL
// list has more than one item; otherwise false.
return F.brandCheck(this,G),this[R].urlList.length>1}
// Returns response’s status.
get status(){
// The status getter steps are to return this’s response’s status.
return F.brandCheck(this,G),this[R].status}
// Returns whether response’s status is an ok status.
get ok(){
// The ok getter steps are to return true if this’s response’s status is an
// ok status; otherwise false.
return F.brandCheck(this,G),this[R].status>=200&&this[R].status<=299}
// Returns response’s status message.
get statusText(){
// The statusText getter steps are to return this’s response’s status
// message.
return F.brandCheck(this,G),this[R].statusText}
// Returns response’s headers as Headers.
get headers(){
// The headers getter steps are to return this’s headers.
return F.brandCheck(this,G),this[b]}get body(){return F.brandCheck(this,G),this[R].body?this[R].body.stream:null}get bodyUsed(){return F.brandCheck(this,G),!!this[R].body&&B.isDisturbed(this[R].body.stream)}
// Returns a clone of response.
clone(){
// 1. If this is unusable, then throw a TypeError.
if(F.brandCheck(this,G),Q(this))throw F.errors.exception({header:"Response.clone",message:"Body has already been consumed."});
// 2. Let clonedResponse be the result of cloning this’s response.
const A=Y(this[R]);
// Note: To re-register because of a new stream.
// 3. Return the result of creating a Response object, given
// clonedResponse, this’s headers’s guard, and this’s relevant Realm.
return g&&this[R].body?.stream&&c.register(this,new WeakRef(this[R].body.stream)),W(A,r(this[b]))}[h.inspect.custom](A,e){null===e.depth&&(e.depth=2),e.colors??=!0;const t={status:this.status,statusText:this.statusText,headers:this.headers,body:this.body,bodyUsed:this.bodyUsed,ok:this.ok,redirected:this.redirected,type:this.type,url:this.url};return`Response ${h.formatWithOptions(e,t)}`}}
// https://fetch.spec.whatwg.org/#concept-response-clone
function Y(A){
// To clone a response response, run these steps:
// 1. If response is a filtered response, then return a new identical
// filtered response whose internal response is a clone of response’s
// internal response.
if(A.internalResponse)return V(Y(A.internalResponse),A.type);
// 2. Let newResponse be a copy of response, except for its body.
const e=v({...A,body:null});
// 3. If response’s body is non-null, then set newResponse’s body to the
// result of cloning response’s body.
// 4. Return newResponse.
return null!=A.body&&(e.body=i(e,A.body)),e}function v(A){return{aborted:!1,rangeRequested:!1,timingAllowPassed:!1,requestIncludesCredentials:!1,type:"default",status:200,timingInfo:null,cacheState:"",statusText:"",...A,headersList:A?.headersList?new e(A?.headersList):new e,urlList:A?.urlList?[...A.urlList]:[]}}function J(A){return v({type:"error",status:0,error:w(A)?A:new Error(A?String(A):A),aborted:A&&"AbortError"===A.name})}
// @see https://fetch.spec.whatwg.org/#concept-network-error
function H(A,e){return e={internalResponse:A,...e},new Proxy(A,{get:(A,t)=>t in e?e[t]:A[t],set:(A,t,r)=>(U(!(t in e)),A[t]=r,!0)})}
// https://fetch.spec.whatwg.org/#concept-filtered-response
function V(A,e){
// Set response to the following filtered response with response as its
// internal response, depending on request’s response tainting:
return"basic"===e?H(A,{type:"basic",headersList:A.headersList}):"cors"===e?H(A,{type:"cors",headersList:A.headersList}):"opaque"===e?H(A,{type:"opaque",urlList:Object.freeze([]),status:0,statusText:"",body:null}):"opaqueredirect"===e?H(A,{type:"opaqueredirect",status:0,statusText:"",headersList:[],body:null}):void U(!1)}
// https://fetch.spec.whatwg.org/#appropriate-network-error
// https://whatpr.org/fetch/1392.html#initialize-a-response
function x(A,e,r){
// 1. If init["status"] is not in the range 200 to 599, inclusive, then
//    throw a RangeError.
if(null!==e.status&&(e.status<200||e.status>599))throw new RangeError('init["status"] must be in the range of 200 to 599, inclusive.');
// 2. If init["statusText"] does not match the reason-phrase token production,
//    then throw a TypeError.
if("statusText"in e&&null!=e.statusText&&!C(String(e.statusText)))throw new TypeError("Invalid statusText");
// 3. Set response’s response’s status to init["status"].
// 6. If body was given, then:
if("status"in e&&null!=e.status&&(A[R].status=e.status),
// 4. Set response’s response’s status message to init["statusText"].
"statusText"in e&&null!=e.statusText&&(A[R].statusText=e.statusText),
// 5. If init["headers"] exists, then fill response’s headers with init["headers"].
"headers"in e&&null!=e.headers&&t(A[b],e.headers),r){
// 1. If response's status is a null body status, then throw a TypeError.
if(k.includes(A.status))throw F.errors.exception({header:"Response constructor",message:`Invalid response status code ${A.status}`});
// 2. Set response's body to body's body.
A[R].body=r.body,
// 3. If body's type is non-null and response's header list does not contain
//    `Content-Type`, then append (`Content-Type`, body's type) to response's header list.
null==r.type||A[R].headersList.contains("content-type",!0)||A[R].headersList.append("content-type",r.type,!0)}}
/**
	 * @see https://fetch.spec.whatwg.org/#response-create
	 * @param {any} innerResponse
	 * @param {'request' | 'immutable' | 'request-no-cors' | 'response' | 'none'} guard
	 * @returns {Response}
	 */function W(e,t){const r=new G(M);return r[R]=e,r[b]=new A(M),s(r[b],e.headersList),n(r[b],t),g&&e.body?.stream&&
// If the target (response) is reclaimed, the cleanup callback may be called at some point with
// the held value provided for it (innerResponse.body.stream). The held value can be any value:
// a primitive or an object, even undefined. If the held value is an object, the registry keeps
// a strong reference to it (so it can pass it to the cleanup callback later). Reworded from
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/FinalizationRegistry
c.register(r,new WeakRef(e.body.stream)),r}return a(G),Object.defineProperties(G.prototype,{type:I,url:I,status:I,ok:I,redirected:I,statusText:I,headers:I,clone:I,body:I,bodyUsed:I,[Symbol.toStringTag]:{value:"Response",configurable:!0}}),Object.defineProperties(G,{json:I,redirect:I,error:I}),F.converters.ReadableStream=F.interfaceConverter(ReadableStream),F.converters.FormData=F.interfaceConverter(S),F.converters.URLSearchParams=F.interfaceConverter(URLSearchParams),
// https://fetch.spec.whatwg.org/#typedefdef-xmlhttprequestbodyinit
F.converters.XMLHttpRequestBodyInit=function(A,e,t){return"string"==typeof A?F.converters.USVString(A,e,t):f(A)?F.converters.Blob(A,e,t,{strict:!1}):ArrayBuffer.isView(A)||L.isArrayBuffer(A)?F.converters.BufferSource(A,e,t):B.isFormDataLike(A)?F.converters.FormData(A,e,t,{strict:!1}):A instanceof URLSearchParams?F.converters.URLSearchParams(A,e,t):F.converters.DOMString(A,e,t)},
// https://fetch.spec.whatwg.org/#bodyinit
F.converters.BodyInit=function(A,e,t){return A instanceof ReadableStream?F.converters.ReadableStream(A,e,t):
// Note: the spec doesn't include async iterables,
// this is an undici extension.
A?.[Symbol.asyncIterator]?A:F.converters.XMLHttpRequestBodyInit(A,e,t)},F.converters.ResponseInit=F.dictionaryConverter([{key:"status",converter:F.converters["unsigned short"],defaultValue:()=>200},{key:"statusText",converter:F.converters.ByteString,defaultValue:()=>""},{key:"headers",converter:F.converters.HeadersInit}]),Xt={isNetworkError:function(A){
// A network error is a response whose type is "error",
return"error"===A.type&&
// status is 0
0===A.status},makeNetworkError:J,makeResponse:v,makeAppropriateNetworkError:function(A,e=null){
// 2. Return an aborted network error if fetchParams is aborted;
// otherwise return a network error.
// 1. Assert: fetchParams is canceled.
return U(u(A)),d(A)?J(Object.assign(new DOMException("The operation was aborted.","AbortError"),{cause:e})):J(Object.assign(new DOMException("Request was cancelled."),{cause:e}))},filterResponse:V,Response:G,cloneResponse:Y,fromInnerResponse:W}}function fn(){if($t)return jt;$t=1;const{kConnected:A,kSize:e}=BA();class t{constructor(A){this.value=A}deref(){return 0===this.value[A]&&0===this.value[e]?void 0:this.value}}class r{constructor(A){this.finalizer=A}register(t,r){t.on&&t.on("disconnect",()=>{0===t[A]&&0===t[e]&&this.finalizer(r)})}unregister(A){}}return jt=function(){
// FIXME: remove workaround when the Node bug is backported to v18
// https://github.com/nodejs/node/issues/49344#issuecomment-1741776308
return process.env.NODE_V8_COVERAGE&&process.version.startsWith("v18")?(process._rawDebug("Using compatibility WeakRef and FinalizationRegistry"),{WeakRef:t,FinalizationRegistry:r}):{WeakRef,FinalizationRegistry}}}
/* globals AbortController */function pn(){if(er)return Ar;er=1;const{extractBody:A,mixinBody:e,cloneBody:t,bodyUnusable:r}=Ge(),{Headers:n,fill:s,HeadersList:o,setHeadersGuard:i,getHeadersGuard:a,setHeadersList:g,getHeadersList:c}=un(),{FinalizationRegistry:Q}=fn()(),B=CA(),h=l,{isValidHTTPToken:I,sameOrigin:C,environmentSettingsObject:u}=Me(),{forbiddenMethodsSet:f,corsSafeListedMethodsSet:p,referrerPolicy:w,requestRedirect:y,requestMode:D,requestCredentials:m,requestCache:k,requestDuplex:R}=be(),{kEnumerableProperty:b,normalizedMethodRecordsBase:F,normalizedMethodRecords:S}=B,{kHeaders:N,kSignal:M,kState:U,kDispatcher:L}=Ue(),{webidl:T}=Ne(),{URLSerializer:G}=Se(),{kConstruct:Y}=BA(),v=E,{getMaxListeners:J,setMaxListeners:H,getEventListeners:V,defaultMaxListeners:x}=d,W=Symbol("abortController"),O=new Q(({signal:A,abort:e})=>{A.removeEventListener("abort",e)}),P=new WeakMap;function q(A){return function e(){const t=A.deref();if(void 0!==t){
// Currently, there is a problem with FinalizationRegistry.
// https://github.com/nodejs/node/issues/49344
// https://github.com/nodejs/node/issues/47748
// In the case of abort, the first step is to unregister from it.
// If the controller can refer to it, it is still registered.
// It will be removed in the future.
O.unregister(e),
// Unsubscribe a listener.
// FinalizationRegistry will no longer be called, so this must be done.
this.removeEventListener("abort",e),t.abort(this.reason);const A=P.get(t.signal);if(void 0!==A){if(0!==A.size){for(const e of A){const A=e.deref();void 0!==A&&A.abort(this.reason)}A.clear()}P.delete(t.signal)}}}}let Z=!1;
// https://fetch.spec.whatwg.org/#request-class
class z{
// https://fetch.spec.whatwg.org/#dom-request
constructor(e,t={}){if(T.util.markAsUncloneable(this),e===Y)return;const a="Request constructor";T.argumentLengthCheck(arguments,1,a),e=T.converters.RequestInfo(e,a,"input"),t=T.converters.RequestInit(t,a,"init");
// 1. Let request be null.
let Q=null,E=null;
// 2. Let fallbackMode be null.
// 3. Let baseURL be this’s relevant settings object’s API base URL.
const h=u.settingsObject.baseUrl;
// 4. Let signal be null.
let l=null;
// 5. If input is a string, then:
if("string"==typeof e){
// 1. Let parsedURL be the result of parsing input with baseURL.
// 2. If parsedURL is failure, then throw a TypeError.
let A;this[L]=t.dispatcher;try{A=new URL(e,h)}catch(A){throw new TypeError("Failed to parse URL from "+e,{cause:A})}
// 3. If parsedURL includes credentials, then throw a TypeError.
if(A.username||A.password)throw new TypeError("Request cannot be constructed from a URL that includes credentials: "+e);
// 4. Set request to a new request whose URL is parsedURL.
Q=K({urlList:[A]}),
// 5. Set fallbackMode to "cors".
E="cors"}else this[L]=t.dispatcher||e[L],
// 6. Otherwise:
// 7. Assert: input is a Request object.
v(e instanceof z),
// 8. Set request to input’s request.
Q=e[U],
// 9. Set signal to input’s signal.
l=e[M];
// 7. Let origin be this’s relevant settings object’s origin.
const d=u.settingsObject.origin;
// 8. Let window be "client".
let w="client";
// 9. If request’s window is an environment settings object and its origin
// is same origin with origin, then set window to request’s window.
// 10. If init["window"] exists and is non-null, then throw a TypeError.
if("EnvironmentSettingsObject"===Q.window?.constructor?.name&&C(Q.window,d)&&(w=Q.window),null!=t.window)throw new TypeError(`'window' option '${w}' must be null`);
// 11. If init["window"] exists, then set window to "no-window".
"window"in t&&(w="no-window"),
// 12. Set request to a new request with the following properties:
Q=K({
// URL request’s URL.
// undici implementation note: this is set as the first item in request's urlList in makeRequest
// method request’s method.
method:Q.method,
// header list A copy of request’s header list.
// undici implementation note: headersList is cloned in makeRequest
headersList:Q.headersList,
// unsafe-request flag Set.
unsafeRequest:Q.unsafeRequest,
// client This’s relevant settings object.
client:u.settingsObject,
// window window.
window:w,
// priority request’s priority.
priority:Q.priority,
// origin request’s origin. The propagation of the origin is only significant for navigation requests
// being handled by a service worker. In this scenario a request can have an origin that is different
// from the current client.
origin:Q.origin,
// referrer request’s referrer.
referrer:Q.referrer,
// referrer policy request’s referrer policy.
referrerPolicy:Q.referrerPolicy,
// mode request’s mode.
mode:Q.mode,
// credentials mode request’s credentials mode.
credentials:Q.credentials,
// cache mode request’s cache mode.
cache:Q.cache,
// redirect mode request’s redirect mode.
redirect:Q.redirect,
// integrity metadata request’s integrity metadata.
integrity:Q.integrity,
// keepalive request’s keepalive.
keepalive:Q.keepalive,
// reload-navigation flag request’s reload-navigation flag.
reloadNavigation:Q.reloadNavigation,
// history-navigation flag request’s history-navigation flag.
historyNavigation:Q.historyNavigation,
// URL list A clone of request’s URL list.
urlList:[...Q.urlList]});const y=0!==Object.keys(t).length;
// 13. If init is not empty, then:
// 14. If init["referrer"] exists, then:
if(y&&(
// 1. If request’s mode is "navigate", then set it to "same-origin".
"navigate"===Q.mode&&(Q.mode="same-origin"),
// 2. Unset request’s reload-navigation flag.
Q.reloadNavigation=!1,
// 3. Unset request’s history-navigation flag.
Q.historyNavigation=!1,
// 4. Set request’s origin to "client".
Q.origin="client",
// 5. Set request’s referrer to "client"
Q.referrer="client",
// 6. Set request’s referrer policy to the empty string.
Q.referrerPolicy="",
// 7. Set request’s URL to request’s current URL.
Q.url=Q.urlList[Q.urlList.length-1],
// 8. Set request’s URL list to « request’s URL ».
Q.urlList=[Q.url]),void 0!==t.referrer){
// 1. Let referrer be init["referrer"].
const A=t.referrer;
// 2. If referrer is the empty string, then set request’s referrer to "no-referrer".
if(""===A)Q.referrer="no-referrer";else{
// 1. Let parsedReferrer be the result of parsing referrer with
// baseURL.
// 2. If parsedReferrer is failure, then throw a TypeError.
let e;try{e=new URL(A,h)}catch(e){throw new TypeError(`Referrer "${A}" is not a valid URL.`,{cause:e})}
// 3. If one of the following is true
// - parsedReferrer’s scheme is "about" and path is the string "client"
// - parsedReferrer’s origin is not same origin with origin
// then set request’s referrer to "client".
"about:"===e.protocol&&"client"===e.hostname||d&&!C(e,u.settingsObject.baseUrl)?Q.referrer="client":
// 4. Otherwise, set request’s referrer to parsedReferrer.
Q.referrer=e}}
// 15. If init["referrerPolicy"] exists, then set request’s referrer policy
// to it.
// 16. Let mode be init["mode"] if it exists, and fallbackMode otherwise.
let D;
// 17. If mode is "navigate", then throw a TypeError.
if(void 0!==t.referrerPolicy&&(Q.referrerPolicy=t.referrerPolicy),D=void 0!==t.mode?t.mode:E,"navigate"===D)throw T.errors.exception({header:"Request constructor",message:"invalid request mode navigate."});
// 18. If mode is non-null, set request’s mode to mode.
// 21. If request’s cache mode is "only-if-cached" and request’s mode is
// not "same-origin", then throw a TypeError.
if(null!=D&&(Q.mode=D),
// 19. If init["credentials"] exists, then set request’s credentials mode
// to it.
void 0!==t.credentials&&(Q.credentials=t.credentials),
// 18. If init["cache"] exists, then set request’s cache mode to it.
void 0!==t.cache&&(Q.cache=t.cache),"only-if-cached"===Q.cache&&"same-origin"!==Q.mode)throw new TypeError("'only-if-cached' can be set only with 'same-origin' mode");
// 22. If init["redirect"] exists, then set request’s redirect mode to it.
// 25. If init["method"] exists, then:
if(void 0!==t.redirect&&(Q.redirect=t.redirect),
// 23. If init["integrity"] exists, then set request’s integrity metadata to it.
null!=t.integrity&&(Q.integrity=String(t.integrity)),
// 24. If init["keepalive"] exists, then set request’s keepalive to it.
void 0!==t.keepalive&&(Q.keepalive=Boolean(t.keepalive)),void 0!==t.method){
// 1. Let method be init["method"].
let A=t.method;const e=S[A];if(void 0!==e)
// Note: Bypass validation DELETE, GET, HEAD, OPTIONS, POST, PUT, PATCH and these lowercase ones
Q.method=e;else{
// 2. If method is not a method or method is a forbidden method, then
// throw a TypeError.
if(!I(A))throw new TypeError(`'${A}' is not a valid HTTP method.`);const e=A.toUpperCase();if(f.has(e))throw new TypeError(`'${A}' HTTP method is unsupported.`);
// 3. Normalize method.
// https://fetch.spec.whatwg.org/#concept-method-normalize
// Note: must be in uppercase
A=F[e]??A,
// 4. Set request’s method to method.
Q.method=A}Z||"patch"!==Q.method||(process.emitWarning("Using `patch` is highly likely to result in a `405 Method Not Allowed`. `PATCH` is much more likely to succeed.",{code:"UNDICI-FETCH-patch"}),Z=!0)}
// 26. If init["signal"] exists, then set signal to it.
void 0!==t.signal&&(l=t.signal),
// 27. Set this’s request to request.
this[U]=Q;
// 28. Set this’s signal to a new AbortSignal object with this’s relevant
// Realm.
// TODO: could this be simplified with AbortSignal.any
// (https://dom.spec.whatwg.org/#dom-abortsignal-any)
const m=new AbortController;
// 29. If signal is not null, then make this’s signal follow signal.
if(this[M]=m.signal,null!=l){if(!l||"boolean"!=typeof l.aborted||"function"!=typeof l.addEventListener)throw new TypeError("Failed to construct 'Request': member signal is not of type AbortSignal.");if(l.aborted)m.abort(l.reason);else{
// Keep a strong ref to ac while request object
// is alive. This is needed to prevent AbortController
// from being prematurely garbage collected.
// See, https://github.com/nodejs/undici/issues/1926.
this[W]=m;const A=q(new WeakRef(m));
// Third-party AbortControllers may not work with these.
// See, https://github.com/nodejs/undici/pull/1910#issuecomment-1464495619.
try{
// If the max amount of listeners is equal to the default, increase it
// This is only available in node >= v19.9.0
("function"==typeof J&&J(l)===x||V(l,"abort").length>=x)&&H(1500,l)}catch{}B.addAbortListener(l,A),
// The third argument must be a registry key to be unregistered.
// Without it, you cannot unregister.
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/FinalizationRegistry
// abort is used as the unregister key. (because it is unique)
O.register(m,{signal:l,abort:A},A)}}
// 30. Set this’s headers to a new Headers object with this’s relevant
// Realm, whose header list is request’s header list and guard is
// "request".
// 31. If this’s request’s mode is "no-cors", then:
if(this[N]=new n(Y),g(this[N],Q.headersList),i(this[N],"request"),"no-cors"===D){
// 1. If this’s request’s method is not a CORS-safelisted method,
// then throw a TypeError.
if(!p.has(Q.method))throw new TypeError(`'${Q.method} is unsupported in no-cors mode.`);
// 2. Set this’s headers’s guard to "request-no-cors".
i(this[N],"request-no-cors")}
// 32. If init is not empty, then:
if(y){
/** @type {HeadersList} */
const A=c(this[N]),e=void 0!==t.headers?t.headers:new o(A);
// 1. Let headers be a copy of this’s headers and its associated header
// list.
// 2. If init["headers"] exists, then set headers to init["headers"].
// 4. If headers is a Headers object, then for each header in its header
// list, append header’s name/header’s value to this’s headers.
if(
// 3. Empty this’s headers’s header list.
A.clear(),e instanceof o){for(const{name:t,value:r}of e.rawValues())A.append(t,r,!1);
// Note: Copy the `set-cookie` meta-data.
A.cookies=e.cookies}else
// 5. Otherwise, fill this’s headers with headers.
s(this[N],e)}
// 33. Let inputBody be input’s request’s body if input is a Request
// object; otherwise null.
const k=e instanceof z?e[U].body:null;
// 34. If either init["body"] exists and is non-null or inputBody is
// non-null, and request’s method is `GET` or `HEAD`, then throw a
// TypeError.
if(!(null==t.body&&null==k||"GET"!==Q.method&&"HEAD"!==Q.method))throw new TypeError("Request with GET/HEAD method cannot have body.");
// 35. Let initBody be null.
let R=null;
// 36. If init["body"] exists and is non-null, then:
if(null!=t.body){
// 1. Let Content-Type be null.
// 2. Set initBody and Content-Type to the result of extracting
// init["body"], with keepalive set to request’s keepalive.
const[e,r]=A(t.body,Q.keepalive);R=e,
// 3, If Content-Type is non-null and this’s headers’s header list does
// not contain `Content-Type`, then append `Content-Type`/Content-Type to
// this’s headers.
r&&!c(this[N]).contains("content-type",!0)&&this[N].append("content-type",r)}
// 37. Let inputOrInitBody be initBody if it is non-null; otherwise
// inputBody.
const b=R??k;
// 38. If inputOrInitBody is non-null and inputOrInitBody’s source is
// null, then:
if(null!=b&&null==b.source){
// 1. If initBody is non-null and init["duplex"] does not exist,
//    then throw a TypeError.
if(null!=R&&null==t.duplex)throw new TypeError("RequestInit: duplex option is required when sending a body.");
// 2. If this’s request’s mode is neither "same-origin" nor "cors",
// then throw a TypeError.
if("same-origin"!==Q.mode&&"cors"!==Q.mode)throw new TypeError('If request is made from ReadableStream, mode should be "same-origin" or "cors"');
// 3. Set this’s request’s use-CORS-preflight flag.
Q.useCORSPreflightFlag=!0}
// 39. Let finalBody be inputOrInitBody.
let G=b;
// 40. If initBody is null and inputBody is non-null, then:
if(null==R&&null!=k){
// 1. If input is unusable, then throw a TypeError.
if(r(e))throw new TypeError("Cannot construct a Request with a Request object that has already been used.");
// 2. Set finalBody to the result of creating a proxy for inputBody.
// https://streams.spec.whatwg.org/#readablestream-create-a-proxy
const A=new TransformStream;k.stream.pipeThrough(A),G={source:k.source,length:k.length,stream:A.readable}}
// 41. Set this’s request’s body to finalBody.
this[U].body=G}
// Returns request’s HTTP method, which is "GET" by default.
get method(){
// The method getter steps are to return this’s request’s method.
return T.brandCheck(this,z),this[U].method}
// Returns the URL of request as a string.
get url(){
// The url getter steps are to return this’s request’s URL, serialized.
return T.brandCheck(this,z),G(this[U].url)}
// Returns a Headers object consisting of the headers associated with request.
// Note that headers added in the network layer by the user agent will not
// be accounted for in this object, e.g., the "Host" header.
get headers(){
// The headers getter steps are to return this’s headers.
return T.brandCheck(this,z),this[N]}
// Returns the kind of resource requested by request, e.g., "document"
// or "script".
get destination(){
// The destination getter are to return this’s request’s destination.
return T.brandCheck(this,z),this[U].destination}
// Returns the referrer of request. Its value can be a same-origin URL if
// explicitly set in init, the empty string to indicate no referrer, and
// "about:client" when defaulting to the global’s default. This is used
// during fetching to determine the value of the `Referer` header of the
// request being made.
get referrer(){
// 1. If this’s request’s referrer is "no-referrer", then return the
// empty string.
return T.brandCheck(this,z),"no-referrer"===this[U].referrer?"":
// 2. If this’s request’s referrer is "client", then return
// "about:client".
"client"===this[U].referrer?"about:client":this[U].referrer.toString()}
// Returns the referrer policy associated with request.
// This is used during fetching to compute the value of the request’s
// referrer.
get referrerPolicy(){
// The referrerPolicy getter steps are to return this’s request’s referrer policy.
return T.brandCheck(this,z),this[U].referrerPolicy}
// Returns the mode associated with request, which is a string indicating
// whether the request will use CORS, or will be restricted to same-origin
// URLs.
get mode(){
// The mode getter steps are to return this’s request’s mode.
return T.brandCheck(this,z),this[U].mode}
// Returns the credentials mode associated with request,
// which is a string indicating whether credentials will be sent with the
// request always, never, or only when sent to a same-origin URL.
get credentials(){
// The credentials getter steps are to return this’s request’s credentials mode.
return this[U].credentials}
// Returns the cache mode associated with request,
// which is a string indicating how the request will
// interact with the browser’s cache when fetching.
get cache(){
// The cache getter steps are to return this’s request’s cache mode.
return T.brandCheck(this,z),this[U].cache}
// Returns the redirect mode associated with request,
// which is a string indicating how redirects for the
// request will be handled during fetching. A request
// will follow redirects by default.
get redirect(){
// The redirect getter steps are to return this’s request’s redirect mode.
return T.brandCheck(this,z),this[U].redirect}
// Returns request’s subresource integrity metadata, which is a
// cryptographic hash of the resource being fetched. Its value
// consists of multiple hashes separated by whitespace. [SRI]
get integrity(){
// The integrity getter steps are to return this’s request’s integrity
// metadata.
return T.brandCheck(this,z),this[U].integrity}
// Returns a boolean indicating whether or not request can outlive the
// global in which it was created.
get keepalive(){
// The keepalive getter steps are to return this’s request’s keepalive.
return T.brandCheck(this,z),this[U].keepalive}
// Returns a boolean indicating whether or not request is for a reload
// navigation.
get isReloadNavigation(){
// The isReloadNavigation getter steps are to return true if this’s
// request’s reload-navigation flag is set; otherwise false.
return T.brandCheck(this,z),this[U].reloadNavigation}
// Returns a boolean indicating whether or not request is for a history
// navigation (a.k.a. back-forward navigation).
get isHistoryNavigation(){
// The isHistoryNavigation getter steps are to return true if this’s request’s
// history-navigation flag is set; otherwise false.
return T.brandCheck(this,z),this[U].historyNavigation}
// Returns the signal associated with request, which is an AbortSignal
// object indicating whether or not request has been aborted, and its
// abort event handler.
get signal(){
// The signal getter steps are to return this’s signal.
return T.brandCheck(this,z),this[M]}get body(){return T.brandCheck(this,z),this[U].body?this[U].body.stream:null}get bodyUsed(){return T.brandCheck(this,z),!!this[U].body&&B.isDisturbed(this[U].body.stream)}get duplex(){return T.brandCheck(this,z),"half"}
// Returns a clone of request.
clone(){
// 1. If this is unusable, then throw a TypeError.
if(T.brandCheck(this,z),r(this))throw new TypeError("unusable");
// 2. Let clonedRequest be the result of cloning this’s request.
const A=X(this[U]),e=new AbortController;
// 3. Let clonedRequestObject be the result of creating a Request object,
// given clonedRequest, this’s headers’s guard, and this’s relevant Realm.
// 4. Make clonedRequestObject’s signal follow this’s signal.
if(this.signal.aborted)e.abort(this.signal.reason);else{let A=P.get(this.signal);void 0===A&&(A=new Set,P.set(this.signal,A));const t=new WeakRef(e);A.add(t),B.addAbortListener(e.signal,q(t))}
// 4. Return clonedRequestObject.
return _(A,e.signal,a(this[N]))}[h.inspect.custom](A,e){null===e.depth&&(e.depth=2),e.colors??=!0;const t={method:this.method,url:this.url,headers:this.headers,destination:this.destination,referrer:this.referrer,referrerPolicy:this.referrerPolicy,mode:this.mode,credentials:this.credentials,cache:this.cache,redirect:this.redirect,integrity:this.integrity,keepalive:this.keepalive,isReloadNavigation:this.isReloadNavigation,isHistoryNavigation:this.isHistoryNavigation,signal:this.signal};return`Request ${h.formatWithOptions(e,t)}`}}
// https://fetch.spec.whatwg.org/#requests
function K(A){return{method:A.method??"GET",localURLsOnly:A.localURLsOnly??!1,unsafeRequest:A.unsafeRequest??!1,body:A.body??null,client:A.client??null,reservedClient:A.reservedClient??null,replacesClientId:A.replacesClientId??"",window:A.window??"client",keepalive:A.keepalive??!1,serviceWorkers:A.serviceWorkers??"all",initiator:A.initiator??"",destination:A.destination??"",priority:A.priority??null,origin:A.origin??"client",policyContainer:A.policyContainer??"client",referrer:A.referrer??"client",referrerPolicy:A.referrerPolicy??"",mode:A.mode??"no-cors",useCORSPreflightFlag:A.useCORSPreflightFlag??!1,credentials:A.credentials??"same-origin",useCredentials:A.useCredentials??!1,cache:A.cache??"default",redirect:A.redirect??"follow",integrity:A.integrity??"",cryptoGraphicsNonceMetadata:A.cryptoGraphicsNonceMetadata??"",parserMetadata:A.parserMetadata??"",reloadNavigation:A.reloadNavigation??!1,historyNavigation:A.historyNavigation??!1,userActivation:A.userActivation??!1,taintedOrigin:A.taintedOrigin??!1,redirectCount:A.redirectCount??0,responseTainting:A.responseTainting??"basic",preventNoCacheCacheControlHeaderModification:A.preventNoCacheCacheControlHeaderModification??!1,done:A.done??!1,timingAllowFailed:A.timingAllowFailed??!1,urlList:A.urlList,url:A.urlList[0],headersList:A.headersList?new o(A.headersList):new o}}
// https://fetch.spec.whatwg.org/#concept-request-clone
function X(A){
// To clone a request request, run these steps:
// 1. Let newRequest be a copy of request, except for its body.
const e=K({...A,body:null});
// 2. If request’s body is non-null, set newRequest’s body to the
// result of cloning request’s body.
// 3. Return newRequest.
return null!=A.body&&(e.body=t(e,A.body)),e}
/**
	 * @see https://fetch.spec.whatwg.org/#request-create
	 * @param {any} innerRequest
	 * @param {AbortSignal} signal
	 * @param {'request' | 'immutable' | 'request-no-cors' | 'response' | 'none'} guard
	 * @returns {Request}
	 */function _(A,e,t){const r=new z(Y);return r[U]=A,r[M]=e,r[N]=new n(Y),g(r[N],A.headersList),i(r[N],t),r}return e(z),Object.defineProperties(z.prototype,{method:b,url:b,headers:b,redirect:b,clone:b,signal:b,duplex:b,destination:b,body:b,bodyUsed:b,isHistoryNavigation:b,isReloadNavigation:b,keepalive:b,integrity:b,cache:b,credentials:b,attribute:b,referrerPolicy:b,referrer:b,mode:b,[Symbol.toStringTag]:{value:"Request",configurable:!0}}),T.converters.Request=T.interfaceConverter(z),
// https://fetch.spec.whatwg.org/#requestinfo
T.converters.RequestInfo=function(A,e,t){return"string"==typeof A?T.converters.USVString(A,e,t):A instanceof z?T.converters.Request(A,e,t):T.converters.USVString(A,e,t)},T.converters.AbortSignal=T.interfaceConverter(AbortSignal),
// https://fetch.spec.whatwg.org/#requestinit
T.converters.RequestInit=T.dictionaryConverter([{key:"method",converter:T.converters.ByteString},{key:"headers",converter:T.converters.HeadersInit},{key:"body",converter:T.nullableConverter(T.converters.BodyInit)},{key:"referrer",converter:T.converters.USVString},{key:"referrerPolicy",converter:T.converters.DOMString,
// https://w3c.github.io/webappsec-referrer-policy/#referrer-policy
allowedValues:w},{key:"mode",converter:T.converters.DOMString,
// https://fetch.spec.whatwg.org/#concept-request-mode
allowedValues:D},{key:"credentials",converter:T.converters.DOMString,
// https://fetch.spec.whatwg.org/#requestcredentials
allowedValues:m},{key:"cache",converter:T.converters.DOMString,
// https://fetch.spec.whatwg.org/#requestcache
allowedValues:k},{key:"redirect",converter:T.converters.DOMString,
// https://fetch.spec.whatwg.org/#requestredirect
allowedValues:y},{key:"integrity",converter:T.converters.DOMString},{key:"keepalive",converter:T.converters.boolean},{key:"signal",converter:T.nullableConverter(A=>T.converters.AbortSignal(A,"RequestInit","signal",{strict:!1}))},{key:"window",converter:T.converters.any},{key:"duplex",converter:T.converters.DOMString,allowedValues:R},{key:"dispatcher",// undici specific option
converter:T.converters.any}]),Ar={Request:z,makeRequest:K,fromInnerRequest:_,cloneRequest:X}}function wn(){if(rr)return tr;rr=1;const{makeNetworkError:A,makeAppropriateNetworkError:e,filterResponse:t,makeResponse:r,fromInnerResponse:n}=dn(),{HeadersList:s}=un(),{Request:o,cloneRequest:i}=pn(),a=w,{bytesMatch:g,makePolicyContainer:c,clonePolicyContainer:Q,requestBadPort:B,TAOCheck:l,appendRequestOriginHeader:u,responseLocationURL:f,requestCurrentURL:p,setRequestReferrerPolicyOnRedirect:y,tryUpgradeRequestToAPotentiallyTrustworthyURL:D,createOpaqueTimingInfo:m,appendFetchMetadata:k,corsCheck:R,crossOriginResourcePolicyCheck:b,determineRequestsReferrer:F,coarsenedSharedCurrentTime:S,createDeferredPromise:N,isBlobLike:M,sameOrigin:U,isCancelled:L,isAborted:T,isErrorLike:G,fullyReadBody:Y,readableStreamClose:v,isomorphicEncode:J,urlIsLocal:H,urlIsHttpHttpsScheme:V,urlHasHttpsScheme:x,clampAndCoarsenConnectionTimingInfo:W,simpleRangeHeaderValue:O,buildContentRange:P,createInflate:q,extractMimeType:Z}=Me(),{kState:z,kDispatcher:K}=Ue(),X=E,{safelyExtractBody:_,extractBody:j}=Ge(),{redirectStatusSet:$,nullBodyStatus:AA,safeMethodsSet:eA,requestBodyHeader:tA,subresourceSet:rA}=be(),nA=d,{Readable:sA,pipeline:oA,finished:iA}=I,{addAbortListener:aA,isErrored:gA,isReadable:cA,bufferToLowerCasedHeaderName:QA}=CA(),{dataURLProcessor:EA,serializeAMimeType:BA,minimizeSupportedMimeType:hA}=Se(),{getGlobalDispatcher:IA}=En(),{webidl:lA}=Ne(),{STATUS_CODES:uA}=h,dA=["GET","HEAD"],fA="undefined"!=typeof __UNDICI_IS_NODE__||"undefined"!=typeof esbuildDetection?"node":"undici";
/** @type {import('buffer').resolveObjectURL} */
let pA;class wA extends nA{constructor(A){super(),this.dispatcher=A,this.connection=null,this.dump=!1,this.state="ongoing"}terminate(A){"ongoing"===this.state&&(this.state="terminated",this.connection?.destroy(A),this.emit("terminated",A))}
// https://fetch.spec.whatwg.org/#fetch-controller-abort
abort(A){"ongoing"===this.state&&(
// 1. Set controller’s state to "aborted".
this.state="aborted",
// 2. Let fallbackError be an "AbortError" DOMException.
// 3. Set error to fallbackError if it is not given.
A||(A=new DOMException("The operation was aborted.","AbortError")),
// 4. Let serializedError be StructuredSerialize(error).
//    If that threw an exception, catch it, and let
//    serializedError be StructuredSerialize(fallbackError).
// 5. Set controller’s serialized abort reason to serializedError.
this.serializedAbortReason=A,this.connection?.destroy(A),this.emit("terminated",A))}}function yA(A){DA(A,"fetch")}
// https://fetch.spec.whatwg.org/#fetch-method
// https://fetch.spec.whatwg.org/#finalize-and-report-timing
function DA(A,e="other"){
// 1. If response is an aborted network error, then return.
if("error"===A.type&&A.aborted)return;
// 2. If response’s URL list is null or empty, then return.
if(!A.urlList?.length)return;
// 3. Let originalURL be response’s URL list[0].
const t=A.urlList[0];
// 4. Let timingInfo be response’s timing info.
let r=A.timingInfo,n=A.cacheState;
// 5. Let cacheState be response’s cache state.
// 6. If originalURL’s scheme is not an HTTP(S) scheme, then return.
V(t)&&null!==r&&(
// 8. If response’s timing allow passed flag is not set, then:
A.timingAllowPassed||(
//  1. Set timingInfo to a the result of creating an opaque timing info for timingInfo.
r=m({startTime:r.startTime}),
//  2. Set cacheState to the empty string.
n=""),
// 9. Set timingInfo’s end time to the coarsened shared current time
// given global’s relevant settings object’s cross-origin isolated
// capability.
// TODO: given global’s relevant settings object’s cross-origin isolated
// capability?
r.endTime=S(),
// 10. Set response’s timing info to timingInfo.
A.timingInfo=r,
// 11. Mark resource timing for timingInfo, originalURL, initiatorType,
// global, and cacheState.
mA(r,t.href,e,globalThis,n));
// 7. If timingInfo is null, then return.
}
// https://w3c.github.io/resource-timing/#dfn-mark-resource-timing
const mA=performance.markResourceTiming;
// https://fetch.spec.whatwg.org/#abort-fetch
function kA(A,e,t,r){
// 3. If responseObject is null, then return.
if(
// 1. Reject promise with error.
A&&
// We might have already resolved the promise at this stage
A.reject(r),
// 2. If request’s body is not null and is readable, then cancel request’s
// body with error.
null!=e.body&&cA(e.body?.stream)&&e.body.stream.cancel(r).catch(A=>{if("ERR_INVALID_STATE"!==A.code)throw A}),null==t)return;
// 4. Let response be responseObject’s response.
const n=t[z];
// 5. If response’s body is not null and is readable, then error response’s
// body with error.
null!=n.body&&cA(n.body?.stream)&&n.body.stream.cancel(r).catch(A=>{if("ERR_INVALID_STATE"!==A.code)throw A})}
// https://fetch.spec.whatwg.org/#fetching
function RA({request:A,processRequestBodyChunkLength:e,processRequestEndOfBody:t,processResponse:r,processResponseEndOfBody:n,processResponseConsumeBody:s,useParallelQueue:o=!1,dispatcher:i=IA()}){
// Ensure that the dispatcher is set accordingly
X(i);
// 1. Let taskDestination be null.
let a=null,g=!1;
// 2. Let crossOriginIsolatedCapability be false.
// 3. If request’s client is non-null, then:
null!=A.client&&(
// 1. Set taskDestination to request’s client’s global object.
a=A.client.globalObject,
// 2. Set crossOriginIsolatedCapability to request’s client’s cross-origin
// isolated capability.
g=A.client.crossOriginIsolatedCapability);
// 4. If useParallelQueue is true, then set taskDestination to the result of
// starting a new parallel queue.
// TODO
// 5. Let timingInfo be a new fetch timing info whose start time and
// post-redirect start time are the coarsened shared current time given
// crossOriginIsolatedCapability.
const E=S(g),B=m({startTime:E}),h={controller:new wA(i),request:A,timingInfo:B,processRequestBodyChunkLength:e,processRequestEndOfBody:t,processResponse:r,processResponseConsumeBody:s,processResponseEndOfBody:n,taskDestination:a,crossOriginIsolatedCapability:g};
// 12. If request’s header list does not contain `Accept`, then:
if(
// 7. If request’s body is a byte sequence, then set request’s body to
//    request’s body as a body.
// NOTE: Since fetching is only called from fetch, body should already be
// extracted.
X(!A.body||A.body.stream),
// 8. If request’s window is "client", then set request’s window to request’s
// client, if request’s client’s global object is a Window object; otherwise
// "no-window".
"client"===A.window&&(
// TODO: What if request.client is null?
A.window="Window"===A.client?.globalObject?.constructor?.name?A.client:"no-window"),
// 9. If request’s origin is "client", then set request’s origin to request’s
// client’s origin.
"client"===A.origin&&(A.origin=A.client.origin),
// 10. If all of the following conditions are true:
// TODO
// 11. If request’s policy container is "client", then:
"client"===A.policyContainer&&(
// 1. If request’s client is non-null, then set request’s policy
// container to a clone of request’s client’s policy container. [HTML]
null!=A.client?A.policyContainer=Q(A.client.policyContainer):
// 2. Otherwise, set request’s policy container to a new policy
// container.
A.policyContainer=c()),!A.headersList.contains("accept",!0)){
// 1. Let value be `*/*`.
const e="*/*";
// 2. A user agent should set value to the first matching statement, if
// any, switching on request’s destination:
// "document"
// "frame"
// "iframe"
// `text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8`
// "image"
// `image/png,image/svg+xml,image/*;q=0.8,*/*;q=0.5`
// "style"
// `text/css,*/*;q=0.1`
// TODO
// 3. Append `Accept`/value to request’s header list.
A.headersList.append("accept",e,!0)}
// 13. If request’s header list does not contain `Accept-Language`, then
// user agents should append `Accept-Language`/an appropriate value to
// request’s header list.
// 17. Return fetchParam's controller
return A.headersList.contains("accept-language",!0)||A.headersList.append("accept-language","*",!0),A.priority,rA.has(A.destination),
// 16. Run main fetch given fetchParams.
bA(h).catch(A=>{h.controller.terminate(A)}),h.controller}
// https://fetch.spec.whatwg.org/#concept-main-fetch
async function bA(e,r=!1){
// 1. Let request be fetchParams’s request.
const n=e.request;
// 2. Let response be null.
let s=null;
// 3. If request’s local-URLs-only flag is set and request’s current URL is
// not local, then set response to a network error.
// 12. If recursive is true, then return response.
if(n.localURLsOnly&&!H(p(n))&&(s=A("local URLs only")),
// 4. Run report Content Security Policy violations for request.
// TODO
// 5. Upgrade request to a potentially trustworthy URL, if appropriate.
D(n),
// 6. If should request be blocked due to a bad port, should fetching request
// be blocked as mixed content, or should request be blocked by Content
// Security Policy returns blocked, then set response to a network error.
"blocked"===B(n)&&(s=A("bad port")),
// TODO: should fetching request be blocked as mixed content?
// TODO: should request be blocked by Content Security Policy?
// 7. If request’s referrer policy is the empty string, then set request’s
// referrer policy to request’s policy container’s referrer policy.
""===n.referrerPolicy&&(n.referrerPolicy=n.policyContainer.referrerPolicy),
// 8. If request’s referrer is not "no-referrer", then set request’s
// referrer to the result of invoking determine request’s referrer.
"no-referrer"!==n.referrer&&(n.referrer=F(n)),
// 9. Set request’s current URL’s scheme to "https" if all of the following
// conditions are true:
// - request’s current URL’s scheme is "http"
// - request’s current URL’s host is a domain
// - Matching request’s current URL’s host per Known HSTS Host Domain Name
//   Matching results in either a superdomain match with an asserted
//   includeSubDomains directive or a congruent match (with or without an
//   asserted includeSubDomains directive). [HSTS]
// TODO
// 10. If recursive is false, then run the remaining steps in parallel.
// TODO
// 11. If response is null, then set response to the result of running
// the steps corresponding to the first matching statement:
null===s&&(s=await(async()=>{const t=p(n);
// - request’s current URL’s origin is same origin with request’s origin,
//   and request’s response tainting is "basic"
return U(t,n.url)&&"basic"===n.responseTainting||
// request’s current URL’s scheme is "data"
"data:"===t.protocol||
// - request’s mode is "navigate" or "websocket"
"navigate"===n.mode||"websocket"===n.mode?(
// 1. Set request’s response tainting to "basic".
n.responseTainting="basic",await FA(e)):
// request’s mode is "same-origin"
"same-origin"===n.mode?A('request mode cannot be "same-origin"'):
// request’s mode is "no-cors"
"no-cors"===n.mode?
// 1. If request’s redirect mode is not "follow", then return a network
// error.
"follow"!==n.redirect?A('redirect mode cannot be "follow" for "no-cors" request'):(
// 2. Set request’s response tainting to "opaque".
n.responseTainting="opaque",await FA(e)):
// request’s current URL’s scheme is not an HTTP(S) scheme
V(p(n))?(
// - request’s use-CORS-preflight flag is set
// - request’s unsafe-request flag is set and either request’s method is
//   not a CORS-safelisted method or CORS-unsafe request-header names with
//   request’s header list is not empty
//    1. Set request’s response tainting to "cors".
//    2. Let corsWithPreflightResponse be the result of running HTTP fetch
//    given fetchParams and true.
//    3. If corsWithPreflightResponse is a network error, then clear cache
//    entries using request.
//    4. Return corsWithPreflightResponse.
// TODO
// Otherwise
//    1. Set request’s response tainting to "cors".
n.responseTainting="cors",await MA(e)):A("URL scheme must be a HTTP(S) scheme")})()),r)return s;
// 13. If response is not a network error and response is not a filtered
// response, then:
0===s.status||s.internalResponse||(n.responseTainting,
// Set response to the following filtered response with response as its
// internal response, depending on request’s response tainting:
"basic"===n.responseTainting?s=t(s,"basic"):"cors"===n.responseTainting?s=t(s,"cors"):"opaque"===n.responseTainting?s=t(s,"opaque"):X(!1));
// 14. Let internalResponse be response, if response is a network error,
// and response’s internal response otherwise.
let o=0===s.status?s:s.internalResponse;
// 15. If internalResponse’s URL list is empty, then set it to a clone of
// request’s URL list.
// 20. If request’s integrity metadata is not the empty string, then:
if(0===o.urlList.length&&o.urlList.push(...n.urlList),
// 16. If request’s timing allow failed flag is unset, then set
// internalResponse’s timing allow passed flag.
n.timingAllowFailed||(s.timingAllowPassed=!0),
// 17. If response is not a network error and any of the following returns
// blocked
// - should internalResponse to request be blocked as mixed content
// - should internalResponse to request be blocked by Content Security Policy
// - should internalResponse to request be blocked due to its MIME type
// - should internalResponse to request be blocked due to nosniff
// TODO
// 18. If response’s type is "opaque", internalResponse’s status is 206,
// internalResponse’s range-requested flag is set, and request’s header
// list does not contain `Range`, then set response and internalResponse
// to a network error.
"opaque"===s.type&&206===o.status&&o.rangeRequested&&!n.headers.contains("range",!0)&&(s=o=A()),
// 19. If response is not a network error and either request’s method is
// `HEAD` or `CONNECT`, or internalResponse’s status is a null body status,
// set internalResponse’s body to null and disregard any enqueuing toward
// it (if any).
0===s.status||"HEAD"!==n.method&&"CONNECT"!==n.method&&!AA.includes(o.status)||(o.body=null,e.controller.dump=!0),n.integrity){
// 1. Let processBodyError be this step: run fetch finale given fetchParams
// and a network error.
const t=t=>NA(e,A(t));
// 2. If request’s response tainting is "opaque", or response’s body is null,
// then run processBodyError and abort these steps.
if("opaque"===n.responseTainting||null==s.body)return void t(s.error);
// 3. Let processBody given bytes be these steps:
const r=A=>{
// 1. If bytes do not match request’s integrity metadata,
// then run processBodyError and abort these steps. [SRI]
g(A,n.integrity)?(
// 2. Set response’s body to bytes as a body.
s.body=_(A)[0],
// 3. Run fetch finale given fetchParams and response.
NA(e,s)):t("integrity mismatch")};
// 4. Fully read response’s body given processBody and processBodyError.
await Y(s.body,r,t)}else
// 21. Otherwise, run fetch finale given fetchParams and response.
NA(e,s)}
// https://fetch.spec.whatwg.org/#concept-scheme-fetch
// given a fetch params fetchParams
function FA(t){
// Note: since the connection is destroyed on redirect, which sets fetchParams to a
// cancelled state, we do not want this condition to trigger *unless* there have been
// no redirects. See https://github.com/nodejs/undici/issues/1776
// 1. If fetchParams is canceled, then return the appropriate network error for fetchParams.
if(L(t)&&0===t.request.redirectCount)return Promise.resolve(e(t));
// 2. Let request be fetchParams’s request.
const{request:n}=t,{protocol:s}=p(n);
// 3. Switch on request’s current URL’s scheme and run the associated steps:
switch(s){case"about:":
// If request’s current URL’s path is the string "blank", then return a new response
// whose status message is `OK`, header list is « (`Content-Type`, `text/html;charset=utf-8`) »,
// and body is the empty byte sequence as a body.
// Otherwise, return a network error.
return Promise.resolve(A("about scheme is not supported"));case"blob:":{pA||(pA=C.resolveObjectURL);
// 1. Let blobURLEntry be request’s current URL’s blob URL entry.
const e=p(n);
// https://github.com/web-platform-tests/wpt/blob/7b0ebaccc62b566a1965396e5be7bb2bc06f841f/FileAPI/url/resources/fetch-tests.js#L52-L56
// Buffer.resolveObjectURL does not ignore URL queries.
if(0!==e.search.length)return Promise.resolve(A("NetworkError when attempting to fetch resource."));const t=pA(e.toString());
// 2. If request’s method is not `GET`, blobURLEntry is null, or blobURLEntry’s
//    object is not a Blob object, then return a network error.
if("GET"!==n.method||!M(t))return Promise.resolve(A("invalid method"));
// 3. Let blob be blobURLEntry’s object.
// Note: done above
// 4. Let response be a new response.
const s=r(),o=t.size,i=J(`${o}`),a=t.type;
// 5. Let fullLength be blob’s size.
// 8. If request’s header list does not contain `Range`:
// 9. Otherwise:
if(n.headersList.contains("range",!0)){
// 1. Set response’s range-requested flag.
s.rangeRequested=!0;
// 2. Let rangeHeader be the result of getting `Range` from request’s header list.
const e=n.headersList.get("range",!0),r=O(e,!0);
// 3. Let rangeValue be the result of parsing a single range header value given rangeHeader and true.
// 4. If rangeValue is failure, then return a network error.
if("failure"===r)return Promise.resolve(A("failed to fetch the data URL"));
// 5. Let (rangeStart, rangeEnd) be rangeValue.
let{rangeStartValue:i,rangeEndValue:g}=r;
// 6. If rangeStart is null:
// 7. Otherwise:
if(null===i)
// 1. Set rangeStart to fullLength − rangeEnd.
i=o-g,
// 2. Set rangeEnd to rangeStart + rangeEnd − 1.
g=i+g-1;else{
// 1. If rangeStart is greater than or equal to fullLength, then return a network error.
if(i>=o)return Promise.resolve(A("Range start is greater than the blob's size."));
// 2. If rangeEnd is null or rangeEnd is greater than or equal to fullLength, then set
//    rangeEnd to fullLength − 1.
(null===g||g>=o)&&(g=o-1)}
// 8. Let slicedBlob be the result of invoking slice blob given blob, rangeStart,
//    rangeEnd + 1, and type.
const c=t.slice(i,g,a),Q=j(c);
// 9. Let slicedBodyWithType be the result of safely extracting slicedBlob.
// Note: same reason as mentioned above as to why we use extractBody
// 10. Set response’s body to slicedBodyWithType’s body.
s.body=Q[0];
// 11. Let serializedSlicedLength be slicedBlob’s size, serialized and isomorphic encoded.
const E=J(`${c.size}`),B=P(i,g,o);
// 12. Let contentRange be the result of invoking build a content range given rangeStart,
//     rangeEnd, and fullLength.
// 13. Set response’s status to 206.
s.status=206,
// 14. Set response’s status message to `Partial Content`.
s.statusText="Partial Content",
// 15. Set response’s header list to « (`Content-Length`, serializedSlicedLength),
//     (`Content-Type`, type), (`Content-Range`, contentRange) ».
s.headersList.set("content-length",E,!0),s.headersList.set("content-type",a,!0),s.headersList.set("content-range",B,!0)}
// 10. Return response.
else{
// 1. Let bodyWithType be the result of safely extracting blob.
// Note: in the FileAPI a blob "object" is a Blob *or* a MediaSource.
// In node, this can only ever be a Blob. Therefore we can safely
// use extractBody directly.
const A=j(t);
// 2. Set response’s status message to `OK`.
s.statusText="OK",
// 3. Set response’s body to bodyWithType’s body.
s.body=A[0],
// 4. Set response’s header list to « (`Content-Length`, serializedFullLength), (`Content-Type`, type) ».
s.headersList.set("content-length",i,!0),s.headersList.set("content-type",a,!0)}return Promise.resolve(s)}case"data:":{
// 1. Let dataURLStruct be the result of running the
//    data: URL processor on request’s current URL.
const e=p(n),t=EA(e);
// 2. If dataURLStruct is failure, then return a
//    network error.
if("failure"===t)return Promise.resolve(A("failed to fetch the data URL"));
// 3. Let mimeType be dataURLStruct’s MIME type, serialized.
const s=BA(t.mimeType);
// 4. Return a response whose status message is `OK`,
//    header list is « (`Content-Type`, mimeType) »,
//    and body is dataURLStruct’s body as a body.
return Promise.resolve(r({statusText:"OK",headersList:[["content-type",{name:"Content-Type",value:s}]],body:_(t.body)[0]}))}case"file:":
// For now, unfortunate as it is, file URLs are left as an exercise for the reader.
// When in doubt, return a network error.
return Promise.resolve(A("not implemented... yet..."));case"http:":case"https:":
// Return the result of running HTTP fetch given fetchParams.
return MA(t).catch(e=>A(e));default:return Promise.resolve(A("unknown scheme"))}}
// https://fetch.spec.whatwg.org/#finalize-response
function SA(A,e){
// 1. Set fetchParams’s request’s done flag.
A.request.done=!0,
// 2, If fetchParams’s process response done is not null, then queue a fetch
// task to run fetchParams’s process response done given response, with
// fetchParams’s task destination.
null!=A.processResponseDone&&queueMicrotask(()=>A.processResponseDone(e))}
// https://fetch.spec.whatwg.org/#fetch-finale
function NA(A,e){
// 1. Let timingInfo be fetchParams’s timing info.
let t=A.timingInfo;
// 2. If response is not a network error and fetchParams’s request’s client is a secure context,
//    then set timingInfo’s server-timing headers to the result of getting, decoding, and splitting
//    `Server-Timing` from response’s internal response’s header list.
// TODO
// 3. Let processResponseEndOfBody be the following steps:
const r=()=>{
// 1. Let unsafeEndTime be the unsafe shared current time.
const r=Date.now();// ?
// 2. If fetchParams’s request’s destination is "document", then set fetchParams’s controller’s
//    full timing info to fetchParams’s timing info.
"document"===A.request.destination&&(A.controller.fullTimingInfo=t),
// 3. Set fetchParams’s controller’s report timing steps to the following steps given a global object global:
A.controller.reportTimingSteps=()=>{
// 1. If fetchParams’s request’s URL’s scheme is not an HTTP(S) scheme, then return.
if("https:"!==A.request.url.protocol)return;
// 2. Set timingInfo’s end time to the relative high resolution time given unsafeEndTime and global.
t.endTime=r;
// 3. Let cacheState be response’s cache state.
let n=e.cacheState;
// 4. Let bodyInfo be response’s body info.
const s=e.bodyInfo;
// 5. If response’s timing allow passed flag is not set, then set timingInfo to the result of creating an
//    opaque timing info for timingInfo and set cacheState to the empty string.
e.timingAllowPassed||(t=m(t),n="");
// 6. Let responseStatus be 0.
let o=0;
// 7. If fetchParams’s request’s mode is not "navigate" or response’s has-cross-origin-redirects is false:
if("navigator"!==A.request.mode||!e.hasCrossOriginRedirects){
// 1. Set responseStatus to response’s status.
o=e.status;
// 2. Let mimeType be the result of extracting a MIME type from response’s header list.
const A=Z(e.headersList);
// 3. If mimeType is not failure, then set bodyInfo’s content type to the result of minimizing a supported MIME type given mimeType.
"failure"!==A&&(s.contentType=hA(A))}
// 8. If fetchParams’s request’s initiator type is non-null, then mark resource timing given timingInfo,
//    fetchParams’s request’s URL, fetchParams’s request’s initiator type, global, cacheState, bodyInfo,
//    and responseStatus.
null!=A.request.initiatorType&&
// TODO: update markresourcetiming
mA(t,A.request.url.href,A.request.initiatorType,globalThis,n,s,o)};
// 5. Queue a fetch task to run processResponseEndOfBodyTask with fetchParams’s task destination
queueMicrotask(()=>(
// 1. Set fetchParams’s request’s done flag.
A.request.done=!0,
// 2. If fetchParams’s process response end-of-body is non-null, then run fetchParams’s process
//    response end-of-body given response.
null!=A.processResponseEndOfBody&&queueMicrotask(()=>A.processResponseEndOfBody(e)),void(
// 3. If fetchParams’s request’s initiator type is non-null and fetchParams’s request’s client’s
//    global object is fetchParams’s task destination, then run fetchParams’s controller’s report
//    timing steps given fetchParams’s request’s client’s global object.
null!=A.request.initiatorType&&A.controller.reportTimingSteps())))};
// 4. If fetchParams’s process response is non-null, then queue a fetch task to run fetchParams’s
//    process response given response, with fetchParams’s task destination.
null!=A.processResponse&&queueMicrotask(()=>{A.processResponse(e),A.processResponse=null});
// 5. Let internalResponse be response, if response is a network error; otherwise response’s internal response.
const n="error"===e.type?e:e.internalResponse??e;
// 6. If internalResponse’s body is null, then run processResponseEndOfBody.
// 7. Otherwise:
null==n.body?r():
// mcollina: all the following steps of the specs are skipped.
// The internal transform stream is not needed.
// See https://github.com/nodejs/undici/pull/3093#issuecomment-2050198541
// 1. Let transformStream be a new TransformStream.
// 2. Let identityTransformAlgorithm be an algorithm which, given chunk, enqueues chunk in transformStream.
// 3. Set up transformStream with transformAlgorithm set to identityTransformAlgorithm and flushAlgorithm
//    set to processResponseEndOfBody.
// 4. Set internalResponse’s body’s stream to the result of internalResponse’s body’s stream piped through transformStream.
iA(n.body.stream,()=>{r()})}
// https://fetch.spec.whatwg.org/#http-fetch
async function MA(e){
// 1. Let request be fetchParams’s request.
const t=e.request;
// 2. Let response be null.
let r=null,n=null;
// 3. Let actualResponse be null.
// 4. Let timingInfo be fetchParams’s timing info.
const s=e.timingInfo;
// 5. If request’s service-workers mode is "all", then:
// 6. If response is null, then:
if(t.serviceWorkers,null===r){
// 4. If request’s response tainting is "cors" and a CORS check
// for request and response returns failure, then return a network error.
if(
// 1. If makeCORSPreflight is true and one of these conditions is true:
// TODO
// 2. If request’s redirect mode is "follow", then set request’s
// service-workers mode to "none".
"follow"===t.redirect&&(t.serviceWorkers="none"),
// 3. Set response and actualResponse to the result of running
// HTTP-network-or-cache fetch given fetchParams.
n=r=await UA(e),"cors"===t.responseTainting&&"failure"===R(t,r))return A("cors failure");
// 5. If the TAO check for request and response returns failure, then set
// request’s timing allow failed flag.
"failure"===l(t,r)&&(t.timingAllowFailed=!0)}
// 7. If either request’s response tainting or response’s type
// is "opaque", and the cross-origin resource policy check with
// request’s origin, request’s client, request’s destination,
// and actualResponse returns blocked, then return a network error.
return"opaque"!==t.responseTainting&&"opaque"!==r.type||"blocked"!==b(t.origin,t.client,t.destination,n)?(
// 8. If actualResponse’s status is a redirect status, then:
$.has(n.status)&&(
// 1. If actualResponse’s status is not 303, request’s body is not null,
// and the connection uses HTTP/2, then user agents may, and are even
// encouraged to, transmit an RST_STREAM frame.
// See, https://github.com/whatwg/fetch/issues/1288
"manual"!==t.redirect&&e.controller.connection.destroy(void 0,!1),
// 2. Switch on request’s redirect mode:
"error"===t.redirect?
// Set response to a network error.
r=A("unexpected redirect"):"manual"===t.redirect?
// Set response to an opaque-redirect filtered response whose internal
// response is actualResponse.
// NOTE(spec): On the web this would return an `opaqueredirect` response,
// but that doesn't make sense server side.
// See https://github.com/nodejs/undici/issues/1193.
r=n:"follow"===t.redirect?
// Set response to the result of running HTTP-redirect fetch given
// fetchParams and response.
r=
// https://fetch.spec.whatwg.org/#http-redirect-fetch
await function(e,t){
// 1. Let request be fetchParams’s request.
const r=e.request,n=t.internalResponse?t.internalResponse:t;
// 2. Let actualResponse be response, if response is not a filtered response,
// and response’s internal response otherwise.
// 3. Let locationURL be actualResponse’s location URL given request’s current
// URL’s fragment.
let s;try{
// 4. If locationURL is null, then return response.
if(s=f(n,p(r).hash),null==s)return t}catch(e){
// 5. If locationURL is failure, then return a network error.
return Promise.resolve(A(e))}
// 6. If locationURL’s scheme is not an HTTP(S) scheme, then return a network
// error.
if(!V(s))return Promise.resolve(A("URL scheme must be a HTTP(S) scheme"));
// 7. If request’s redirect count is 20, then return a network error.
if(20===r.redirectCount)return Promise.resolve(A("redirect count exceeded"));
// 8. Increase request’s redirect count by 1.
// 9. If request’s mode is "cors", locationURL includes credentials, and
// request’s origin is not same origin with locationURL’s origin, then return
//  a network error.
if(r.redirectCount+=1,"cors"===r.mode&&(s.username||s.password)&&!U(r,s))return Promise.resolve(A('cross origin not allowed for request mode "cors"'));
// 10. If request’s response tainting is "cors" and locationURL includes
// credentials, then return a network error.
if("cors"===r.responseTainting&&(s.username||s.password))return Promise.resolve(A('URL cannot contain credentials for request mode "cors"'));
// 11. If actualResponse’s status is not 303, request’s body is non-null,
// and request’s body’s source is null, then return a network error.
if(303!==n.status&&null!=r.body&&null==r.body.source)return Promise.resolve(A());
// 12. If one of the following is true
// - actualResponse’s status is 301 or 302 and request’s method is `POST`
// - actualResponse’s status is 303 and request’s method is not `GET` or `HEAD`
if([301,302].includes(n.status)&&"POST"===r.method||303===n.status&&!dA.includes(r.method)){
// then:
// 1. Set request’s method to `GET` and request’s body to null.
r.method="GET",r.body=null;
// 2. For each headerName of request-body-header name, delete headerName from
// request’s header list.
for(const A of tA)r.headersList.delete(A)}
// 13. If request’s current URL’s origin is not same origin with locationURL’s
//     origin, then for each headerName of CORS non-wildcard request-header name,
//     delete headerName from request’s header list.
U(p(r),s)||(
// https://fetch.spec.whatwg.org/#cors-non-wildcard-request-header-name
r.headersList.delete("authorization",!0),
// https://fetch.spec.whatwg.org/#authentication-entries
r.headersList.delete("proxy-authorization",!0),
// "Cookie" and "Host" are forbidden request-headers, which undici doesn't implement.
r.headersList.delete("cookie",!0),r.headersList.delete("host",!0));
// 14. If request’s body is non-null, then set request’s body to the first return
// value of safely extracting request’s body’s source.
null!=r.body&&(X(null!=r.body.source),r.body=_(r.body.source)[0]);
// 15. Let timingInfo be fetchParams’s timing info.
const o=e.timingInfo;
// 16. Set timingInfo’s redirect end time and post-redirect start time to the
// coarsened shared current time given fetchParams’s cross-origin isolated
// capability.
// 17. If timingInfo’s redirect start time is 0, then set timingInfo’s
//  redirect start time to timingInfo’s start time.
o.redirectEndTime=o.postRedirectStartTime=S(e.crossOriginIsolatedCapability),0===o.redirectStartTime&&(o.redirectStartTime=o.startTime);
// 18. Append locationURL to request’s URL list.
// 20. Return the result of running main fetch given fetchParams and true.
return r.urlList.push(s),
// 19. Invoke set request’s referrer policy on redirect on request and
// actualResponse.
y(r,n),bA(e,!0)}
// https://fetch.spec.whatwg.org/#http-network-or-cache-fetch
(e,r):X(!1)),
// 9. Set response’s timing info to timingInfo.
r.timingInfo=s,r):A("blocked")}async function UA(t,n=!1,o=!1){
// 1. Let request be fetchParams’s request.
const g=t.request;
// 2. Let httpFetchParams be null.
let c=null,Q=null,E=null;
// 3. Let httpRequest be null.
// 8. Run these steps, but abort when the ongoing fetch is terminated:
//    1. If request’s window is "no-window" and request’s redirect mode is
//    "error", then set httpFetchParams to fetchParams and httpRequest to
//    request.
"no-window"===g.window&&"error"===g.redirect?(c=t,Q=g):(
// Otherwise:
// 1. Set httpRequest to a clone of request.
Q=i(g),
// 2. Set httpFetchParams to a copy of fetchParams.
c={...t},
// 3. Set httpFetchParams’s request to httpRequest.
c.request=Q);
//    3. Let includeCredentials be true if one of
const B="include"===g.credentials||"same-origin"===g.credentials&&"basic"===g.responseTainting,h=Q.body?Q.body.length:null;
//    4. Let contentLength be httpRequest’s body’s length, if httpRequest’s
//    body is non-null; otherwise null.
//    5. Let contentLengthHeaderValue be null.
let I=null;
//    6. If httpRequest’s body is null and httpRequest’s method is `POST` or
//    `PUT`, then set contentLengthHeaderValue to `0`.
// 9. If aborted, then return the appropriate network error for fetchParams.
// TODO
// 10. If response is null, then:
if(null==Q.body&&["POST","PUT"].includes(Q.method)&&(I="0"),
//    7. If contentLength is non-null, then set contentLengthHeaderValue to
//    contentLength, serialized and isomorphic encoded.
null!=h&&(I=J(`${h}`)),
//    8. If contentLengthHeaderValue is non-null, then append
//    `Content-Length`/contentLengthHeaderValue to httpRequest’s header
//    list.
null!=I&&Q.headersList.append("content-length",I,!0),null!=h&&Q.keepalive,
//    11. If httpRequest’s referrer is a URL, then append
//    `Referer`/httpRequest’s referrer, serialized and isomorphic encoded,
//     to httpRequest’s header list.
Q.referrer instanceof URL&&Q.headersList.append("referer",J(Q.referrer.href),!0),
//    12. Append a request `Origin` header for httpRequest.
u(Q),
//    13. Append the Fetch metadata headers for httpRequest. [FETCH-METADATA]
k(Q),
//    14. If httpRequest’s header list does not contain `User-Agent`, then
//    user agents should append `User-Agent`/default `User-Agent` value to
//    httpRequest’s header list.
Q.headersList.contains("user-agent",!0)||Q.headersList.append("user-agent",fA),
//    15. If httpRequest’s cache mode is "default" and httpRequest’s header
//    list contains `If-Modified-Since`, `If-None-Match`,
//    `If-Unmodified-Since`, `If-Match`, or `If-Range`, then set
//    httpRequest’s cache mode to "no-store".
"default"===Q.cache&&(Q.headersList.contains("if-modified-since",!0)||Q.headersList.contains("if-none-match",!0)||Q.headersList.contains("if-unmodified-since",!0)||Q.headersList.contains("if-match",!0)||Q.headersList.contains("if-range",!0))&&(Q.cache="no-store"),
//    16. If httpRequest’s cache mode is "no-cache", httpRequest’s prevent
//    no-cache cache-control header modification flag is unset, and
//    httpRequest’s header list does not contain `Cache-Control`, then append
//    `Cache-Control`/`max-age=0` to httpRequest’s header list.
"no-cache"!==Q.cache||Q.preventNoCacheCacheControlHeaderModification||Q.headersList.contains("cache-control",!0)||Q.headersList.append("cache-control","max-age=0",!0),
//    17. If httpRequest’s cache mode is "no-store" or "reload", then:
"no-store"!==Q.cache&&"reload"!==Q.cache||(
// 1. If httpRequest’s header list does not contain `Pragma`, then append
// `Pragma`/`no-cache` to httpRequest’s header list.
Q.headersList.contains("pragma",!0)||Q.headersList.append("pragma","no-cache",!0),
// 2. If httpRequest’s header list does not contain `Cache-Control`,
// then append `Cache-Control`/`no-cache` to httpRequest’s header list.
Q.headersList.contains("cache-control",!0)||Q.headersList.append("cache-control","no-cache",!0)),
//    18. If httpRequest’s header list contains `Range`, then append
//    `Accept-Encoding`/`identity` to httpRequest’s header list.
Q.headersList.contains("range",!0)&&Q.headersList.append("accept-encoding","identity",!0),
//    19. Modify httpRequest’s header list per HTTP. Do not append a given
//    header if httpRequest’s header list contains that header’s name.
//    TODO: https://github.com/whatwg/fetch/issues/1285#issuecomment-896560129
Q.headersList.contains("accept-encoding",!0)||(x(p(Q))?Q.headersList.append("accept-encoding","br, gzip, deflate",!0):Q.headersList.append("accept-encoding","gzip, deflate",!0)),Q.headersList.delete("host",!0),Q.cache="no-store","no-store"!==Q.cache&&Q.cache,null==E){
// 1. If httpRequest’s cache mode is "only-if-cached", then return a
// network error.
if("only-if-cached"===Q.cache)return A("only if cached");
// 2. Let forwardResponse be the result of running HTTP-network fetch
// given httpFetchParams, includeCredentials, and isNewConnectionFetch.
const t=
// https://fetch.spec.whatwg.org/#http-network-fetch
await async function(t){X(!t.controller.connection||t.controller.connection.destroyed),t.controller.connection={abort:null,destroyed:!1,destroy(A,e=!0){this.destroyed||(this.destroyed=!0,e&&this.abort?.(A??new DOMException("The operation was aborted.","AbortError")))}};
// 1. Let request be fetchParams’s request.
const n=t.request;
// 2. Let response be null.
let o=null;
// 3. Let timingInfo be fetchParams’s timing info.
const i=t.timingInfo;
// 5. If httpCache is null, then set request’s cache mode to "no-store".
n.cache="no-store",n.mode;
// 9. Run these steps, but abort when the ongoing fetch is terminated:
//    1. If connection is failure, then return a network error.
//    2. Set timingInfo’s final connection timing info to the result of
//    calling clamp and coarsen connection timing info with connection’s
//    timing info, timingInfo’s post-redirect start time, and fetchParams’s
//    cross-origin isolated capability.
//    3. If connection is not an HTTP/2 connection, request’s body is non-null,
//    and request’s body’s source is null, then append (`Transfer-Encoding`,
//    `chunked`) to request’s header list.
//    4. Set timingInfo’s final network-request start time to the coarsened
//    shared current time given fetchParams’s cross-origin isolated
//    capability.
//    5. Set response to the result of making an HTTP request over connection
//    using request with the following caveats:
//        - Follow the relevant requirements from HTTP. [HTTP] [HTTP-SEMANTICS]
//        [HTTP-COND] [HTTP-CACHING] [HTTP-AUTH]
//        - If request’s body is non-null, and request’s body’s source is null,
//        then the user agent may have a buffer of up to 64 kibibytes and store
//        a part of request’s body in that buffer. If the user agent reads from
//        request’s body beyond that buffer’s size and the user agent needs to
//        resend request, then instead return a network error.
//        - Set timingInfo’s final network-response start time to the coarsened
//        shared current time given fetchParams’s cross-origin isolated capability,
//        immediately after the user agent’s HTTP parser receives the first byte
//        of the response (e.g., frame header bytes for HTTP/2 or response status
//        line for HTTP/1.x).
//        - Wait until all the headers are transmitted.
//        - Any responses whose status is in the range 100 to 199, inclusive,
//        and is not 101, are to be ignored, except for the purposes of setting
//        timingInfo’s final network-response start time above.
//    - If request’s header list contains `Transfer-Encoding`/`chunked` and
//    response is transferred via HTTP/1.0 or older, then return a network
//    error.
//    - If the HTTP request results in a TLS client certificate dialog, then:
//        1. If request’s window is an environment settings object, make the
//        dialog available in request’s window.
//        2. Otherwise, return a network error.
// To transmit request’s body body, run these steps:
let g=null;
// 1. If body is null and fetchParams’s process request end-of-body is
// non-null, then queue a fetch task given fetchParams’s process request
// end-of-body and fetchParams’s task destination.
if(null==n.body&&t.processRequestEndOfBody)queueMicrotask(()=>t.processRequestEndOfBody());else if(null!=n.body){
// 2. Otherwise, if body is non-null:
//    1. Let processBodyChunk given bytes be these steps:
const A=async function*(A){
// 1. If the ongoing fetch is terminated, then abort these steps.
L(t)||(yield A,
// 3. If fetchParams’s process request body is non-null, then run
// fetchParams’s process request body given bytes’s length.
t.processRequestBodyChunkLength?.(A.byteLength));
// 2. Run this step in parallel: transmit bytes.
},e=()=>{
// 1. If fetchParams is canceled, then abort these steps.
L(t)||
// 2. If fetchParams’s process request end-of-body is non-null,
// then run fetchParams’s process request end-of-body.
t.processRequestEndOfBody&&t.processRequestEndOfBody()},r=A=>{
// 1. If fetchParams is canceled, then abort these steps.
L(t)||(
// 2. If e is an "AbortError" DOMException, then abort fetchParams’s controller.
"AbortError"===A.name?t.controller.abort():t.controller.terminate(A))};
// 2. Let processEndOfBody be these steps:
// 4. Incrementally read request’s body given processBodyChunk, processEndOfBody,
// processBodyError, and fetchParams’s task destination.
g=async function*(){try{for await(const e of n.body.stream)yield*A(e);e()}catch(A){r(A)}}()}try{
// socket is only provided for websockets
const{body:A,status:e,statusText:n,headersList:s,socket:i}=await h({body:g});if(i)o=r({status:e,statusText:n,headersList:s,socket:i});else{const i=A[Symbol.asyncIterator]();t.controller.next=()=>i.next(),o=r({status:e,statusText:n,headersList:s})}}catch(r){
// 10. If aborted, then:
return"AbortError"===r.name?(
// 1. If connection uses HTTP/2, then transmit an RST_STREAM frame.
t.controller.connection.destroy(),e(t,r)):A(r)}
// 11. Let pullAlgorithm be an action that resumes the ongoing fetch
// if it is suspended.
const c=async()=>{await t.controller.resume()},Q=A=>{
// If the aborted fetch was already terminated, then we do not
// need to do anything.
L(t)||t.controller.abort(A)},E=new ReadableStream({async start(A){t.controller.controller=A},async pull(A){await c()},async cancel(A){await Q(A)},type:"bytes"});
// 12. Let cancelAlgorithm be an algorithm that aborts fetchParams’s
// controller with reason, given reason.
//    2. If aborted, then:
function B(A){
// 2. If fetchParams is aborted, then:
T(t)?(
// 1. Set response’s aborted flag.
o.aborted=!0,
// 2. If stream is readable, then error stream with the result of
//    deserialize a serialized abort reason given fetchParams’s
//    controller’s serialized abort reason and an
//    implementation-defined realm.
cA(E)&&t.controller.controller.error(t.controller.serializedAbortReason)):
// 3. Otherwise, if stream is readable, error stream with a TypeError.
cA(E)&&t.controller.controller.error(new TypeError("terminated",{cause:G(A)?A:void 0})),
// 4. If connection uses HTTP/2, then transmit an RST_STREAM frame.
// 5. Otherwise, the user agent should close connection unless it would be bad for performance to do so.
t.controller.connection.destroy()}
// 20. Return response.
// 17. Run these steps, but abort when the ongoing fetch is terminated:
//    1. Set response’s body to a new body whose stream is stream.
return o.body={stream:E,source:null,length:null},
//    2. If response is not a network error and request’s cache mode is
//    not "no-store", then update response in httpCache for request.
//    TODO
//    3. If includeCredentials is true and the user agent is not configured
//    to block cookies for request (see section 7 of [COOKIES]), then run the
//    "set-cookie-string" parsing algorithm (see section 5.2 of [COOKIES]) on
//    the value of each header whose name is a byte-case-insensitive match for
//    `Set-Cookie` in response’s header list, if any, and request’s current URL.
//    TODO
// 18. If aborted, then:
// TODO
// 19. Run these steps in parallel:
//    1. Run these steps, but abort when fetchParams is canceled:
t.controller.onAborted=B,t.controller.on("terminated",B),t.controller.resume=async()=>{
// 1. While true
for(;;){
// 1-3. See onData...
// 4. Set bytes to the result of handling content codings given
// codings and bytes.
let A,e;try{const{done:e,value:r}=await t.controller.next();if(T(t))break;A=e?void 0:r}catch(r){t.controller.ended&&!i.encodedBodySize?
// zlib doesn't like empty streams.
A=void 0:(A=r,
// err may be propagated from the result of calling readablestream.cancel,
// which might not be an error. https://github.com/nodejs/undici/issues/2009
e=!0)}if(void 0===A)
// 2. Otherwise, if the bytes transmission for response’s message
// body is done normally and stream is readable, then close
// stream, finalize response for fetchParams and response, and
// abort these in-parallel steps.
return v(t.controller.controller),void SA(t,o);
// 5. Increase timingInfo’s decoded body size by bytes’s length.
// 6. If bytes is failure, then terminate fetchParams’s controller.
if(i.decodedBodySize+=A?.byteLength??0,e)return void t.controller.terminate(A);
// 7. Enqueue a Uint8Array wrapping an ArrayBuffer containing bytes
// into stream.
const r=new Uint8Array(A);
// 8. If stream is errored, then terminate the ongoing fetch.
if(r.byteLength&&t.controller.controller.enqueue(r),gA(E))return void t.controller.terminate();
// 9. If stream doesn’t need more data ask the user agent to suspend
// the ongoing fetch.
if(t.controller.controller.desiredSize<=0)return}},o;function h({body:A}){const e=p(n),r=t.controller.dispatcher;
/** @type {import('../..').Agent} */return new Promise((o,g)=>r.dispatch({path:e.pathname+e.search,origin:e.origin,method:n.method,body:r.isMockActive?n.body&&(n.body.source||n.body.stream):A,headers:n.headersList.entries,maxRedirections:0,upgrade:"websocket"===n.mode?"websocket":void 0},{body:null,abort:null,onConnect(A){
// TODO (fix): Do we need connection here?
const{connection:e}=t.controller;
// Set timingInfo’s final connection timing info to the result of calling clamp and coarsen
// connection timing info with connection’s timing info, timingInfo’s post-redirect start
// time, and fetchParams’s cross-origin isolated capability.
// TODO: implement connection timing
i.finalConnectionTimingInfo=W(void 0,i.postRedirectStartTime,t.crossOriginIsolatedCapability),e.destroyed?A(new DOMException("The operation was aborted.","AbortError")):(t.controller.on("terminated",A),this.abort=e.abort=A),
// Set timingInfo’s final network-request start time to the coarsened shared current time given
// fetchParams’s cross-origin isolated capability.
i.finalNetworkRequestStartTime=S(t.crossOriginIsolatedCapability)},onResponseStarted(){
// Set timingInfo’s final network-response start time to the coarsened shared current
// time given fetchParams’s cross-origin isolated capability, immediately after the
// user agent’s HTTP parser receives the first byte of the response (e.g., frame header
// bytes for HTTP/2 or response status line for HTTP/1.x).
i.finalNetworkResponseStartTime=S(t.crossOriginIsolatedCapability)},onHeaders(A,e,t,r){if(A<200)return;let i="";const c=new s;for(let A=0;A<e.length;A+=2)c.append(QA(e[A]),e[A+1].toString("latin1"),!0);i=c.get("location",!0),this.body=new sA({read:t});const Q=[],E=i&&"follow"===n.redirect&&$.has(A);
// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding
if("HEAD"!==n.method&&"CONNECT"!==n.method&&!AA.includes(A)&&!E){
// https://www.rfc-editor.org/rfc/rfc7231#section-3.1.2.1
const A=c.get("content-encoding",!0),e=A?A.toLowerCase().split(","):[],t=5;
// "All content-coding values are case-insensitive..."
/** @type {string[]} */if(e.length>t)return g(new Error(`too many content-encodings in response: ${e.length}, maximum allowed is ${t}`)),!0;for(let A=e.length-1;A>=0;--A){const t=e[A].trim();
// https://www.rfc-editor.org/rfc/rfc9112.html#section-7.2
if("x-gzip"===t||"gzip"===t)Q.push(a.createGunzip({
// Be less strict when decoding compressed responses, since sometimes
// servers send slightly invalid responses that are still accepted
// by common browsers.
// Always using Z_SYNC_FLUSH is what cURL does.
flush:a.constants.Z_SYNC_FLUSH,finishFlush:a.constants.Z_SYNC_FLUSH}));else if("deflate"===t)Q.push(q({flush:a.constants.Z_SYNC_FLUSH,finishFlush:a.constants.Z_SYNC_FLUSH}));else{if("br"!==t){Q.length=0;break}Q.push(a.createBrotliDecompress({flush:a.constants.BROTLI_OPERATION_FLUSH,finishFlush:a.constants.BROTLI_OPERATION_FLUSH}))}}}const B=this.onError.bind(this);return o({status:A,statusText:r,headersList:c,body:Q.length?oA(this.body,...Q,A=>{A&&this.onError(A)}).on("error",B):this.body.on("error",B)}),!0},onData(A){if(t.controller.dump)return;
// 1. If one or more bytes have been transmitted from response’s
// message body, then:
//  1. Let bytes be the transmitted bytes.
const e=A;
//  2. Let codings be the result of extracting header list values
//  given `Content-Encoding` and response’s header list.
//  See pullAlgorithm.
//  3. Increase timingInfo’s encoded body size by bytes’s length.
//  4. See pullAlgorithm...
return i.encodedBodySize+=e.byteLength,this.body.push(e)},onComplete(){this.abort&&t.controller.off("terminated",this.abort),t.controller.onAborted&&t.controller.off("terminated",t.controller.onAborted),t.controller.ended=!0,this.body.push(null)},onError(A){this.abort&&t.controller.off("terminated",this.abort),this.body?.destroy(A),t.controller.terminate(A),g(A)},onUpgrade(A,e,t){if(101!==A)return;const r=new s;for(let A=0;A<e.length;A+=2)r.append(QA(e[A]),e[A+1].toString("latin1"),!0);return o({status:A,statusText:uA[A],headersList:r,socket:t}),!0}}))}}(c,B,o);
// 3. If httpRequest’s method is unsafe and forwardResponse’s status is
// in the range 200 to 399, inclusive, invalidate appropriate stored
// responses in httpCache, as per the "Invalidation" chapter of HTTP
// Caching, and set storedResponse to null. [HTTP-CACHING]
!eA.has(Q.method)&&t.status>=200&&t.status,
// 5. If response is null, then:
null==E&&(
// 1. Set response to forwardResponse.
E=t)}
// 11. Set response’s URL list to a clone of httpRequest’s URL list.
// 14. If response’s status is 401, httpRequest’s response tainting is not
// "cors", includeCredentials is true, and request’s window is an environment
// settings object, then:
// TODO
// 15. If response’s status is 407, then:
if(E.urlList=[...Q.urlList],
// 12. If httpRequest’s header list contains `Range`, then set response’s
// range-requested flag.
Q.headersList.contains("range",!0)&&(E.rangeRequested=!0),
// 13. Set response’s request-includes-credentials to includeCredentials.
E.requestIncludesCredentials=B,407===E.status)
// 1. If request’s window is "no-window", then return a network error.
return"no-window"===g.window?A():
// 2. ???
// 3. If fetchParams is canceled, then return the appropriate network error for fetchParams.
L(t)?e(t):A("proxy authentication required");
// 16. If all of the following are true
if(
// response’s status is 421
421===E.status&&
// isNewConnectionFetch is false
!o&&(
// request’s body is null, or request’s body is non-null and request’s body’s source is non-null
null==g.body||null!=g.body.source)){
// then:
// 1. If fetchParams is canceled, then return the appropriate network error for fetchParams.
if(L(t))return e(t);
// 2. Set response to the result of running HTTP-network-or-cache
// fetch given fetchParams, isAuthenticationFetch, and true.
// TODO (spec): The spec doesn't specify this but we need to cancel
// the active response before we can start a new one.
// https://github.com/whatwg/fetch/issues/1293
t.controller.connection.destroy(),E=await UA(t,n,!0)}
// 18. Return response.
return E}return tr={fetch:function(A,e=void 0){lA.argumentLengthCheck(arguments,1,"globalThis.fetch");
// 1. Let p be a new promise.
let t,r=N();
// 2. Let requestObject be the result of invoking the initial value of
// Request as constructor with input and init as arguments. If this throws
// an exception, reject p with it and return p.
try{t=new o(A,e)}catch(A){return r.reject(A),r.promise}
// 3. Let request be requestObject’s request.
const s=t[z];
// 4. If requestObject’s signal’s aborted flag is set, then:
if(t.signal.aborted)
// 2. Return p.
// 1. Abort the fetch() call with p, request, null, and
//    requestObject’s signal’s abort reason.
return kA(r,s,null,t.signal.reason),r.promise;
// 5. Let globalObject be request’s client’s global object.
const i=s.client.globalObject;
// 6. If globalObject is a ServiceWorkerGlobalScope object, then set
// request’s service-workers mode to "none".
"ServiceWorkerGlobalScope"===i?.constructor?.name&&(s.serviceWorkers="none");
// 7. Let responseObject be null.
let a=null,g=!1,c=null;
// 8. Let relevantRealm be this’s relevant Realm.
// 9. Let locallyAborted be false.
// 14. Return p.
// 11. Add the following abort steps to requestObject’s signal:
return aA(t.signal,()=>{
// 1. Set locallyAborted to true.
g=!0,
// 2. Assert: controller is non-null.
X(null!=c),
// 3. Abort controller with requestObject’s signal’s abort reason.
c.abort(t.signal.reason);const A=a?.deref();
// 4. Abort the fetch() call with p, request, responseObject,
//    and requestObject’s signal’s abort reason.
kA(r,s,A,t.signal.reason)}),c=RA({request:s,processResponseEndOfBody:yA,processResponse:A=>{
// 1. If locallyAborted is true, terminate these substeps.
g||(
// 2. If response’s aborted flag is set, then:
A.aborted?
// 1. Let deserializedError be the result of deserialize a serialized
//    abort reason given controller’s serialized abort reason and
//    relevantRealm.
// 2. Abort the fetch() call with p, request, responseObject, and
//    deserializedError.
kA(r,s,a,c.serializedAbortReason):
// 3. If response is a network error, then reject p with a TypeError
// and terminate these substeps.
"error"!==A.type?(
// 4. Set responseObject to the result of creating a Response object,
// given response, "immutable", and relevantRealm.
a=new WeakRef(n(A,"immutable")),
// 5. Resolve p with responseObject.
r.resolve(a.deref()),r=null):r.reject(new TypeError("fetch failed",{cause:A.error})))},dispatcher:t[K]}),r.promise},Fetch:wA,fetching:RA,finalizeAndReportTiming:DA},tr}function yn(){return sr?nr:(sr=1,nr={kState:Symbol("FileReader state"),kResult:Symbol("FileReader result"),kError:Symbol("FileReader error"),kLastProgressEventFired:Symbol("FileReader last progress event fired timestamp"),kEvents:Symbol("FileReader events"),kAborted:Symbol("FileReader aborted")})}function Dn(){if(Qr)return cr;Qr=1;const{kState:A,kError:e,kResult:t,kAborted:r,kLastProgressEventFired:n}=yn(),{ProgressEvent:s}=function(){if(ir)return or;ir=1;const{webidl:A}=Ne(),e=Symbol("ProgressEvent state");
/**
	 * @see https://xhr.spec.whatwg.org/#progressevent
	 */
class t extends Event{constructor(t,r={}){super(t=A.converters.DOMString(t,"ProgressEvent constructor","type"),r=A.converters.ProgressEventInit(r??{})),this[e]={lengthComputable:r.lengthComputable,loaded:r.loaded,total:r.total}}get lengthComputable(){return A.brandCheck(this,t),this[e].lengthComputable}get loaded(){return A.brandCheck(this,t),this[e].loaded}get total(){return A.brandCheck(this,t),this[e].total}}return A.converters.ProgressEventInit=A.dictionaryConverter([{key:"lengthComputable",converter:A.converters.boolean,defaultValue:()=>!1},{key:"loaded",converter:A.converters["unsigned long long"],defaultValue:()=>0},{key:"total",converter:A.converters["unsigned long long"],defaultValue:()=>0},{key:"bubbles",converter:A.converters.boolean,defaultValue:()=>!1},{key:"cancelable",converter:A.converters.boolean,defaultValue:()=>!1},{key:"composed",converter:A.converters.boolean,defaultValue:()=>!1}]),or={ProgressEvent:t}}(),{getEncoding:o}=gr?ar:(gr=1,ar={getEncoding:
/**
	 * @see https://encoding.spec.whatwg.org/#concept-encoding-get
	 * @param {string|undefined} label
	 */
function(A){if(!A)return"failure";
// 1. Remove any leading and trailing ASCII whitespace from label.
// 2. If label is an ASCII case-insensitive match for any of the
//    labels listed in the table below, then return the
//    corresponding encoding; otherwise return failure.
switch(A.trim().toLowerCase()){case"unicode-1-1-utf-8":case"unicode11utf8":case"unicode20utf8":case"utf-8":case"utf8":case"x-unicode20utf8":return"UTF-8";case"866":case"cp866":case"csibm866":case"ibm866":return"IBM866";case"csisolatin2":case"iso-8859-2":case"iso-ir-101":case"iso8859-2":case"iso88592":case"iso_8859-2":case"iso_8859-2:1987":case"l2":case"latin2":return"ISO-8859-2";case"csisolatin3":case"iso-8859-3":case"iso-ir-109":case"iso8859-3":case"iso88593":case"iso_8859-3":case"iso_8859-3:1988":case"l3":case"latin3":return"ISO-8859-3";case"csisolatin4":case"iso-8859-4":case"iso-ir-110":case"iso8859-4":case"iso88594":case"iso_8859-4":case"iso_8859-4:1988":case"l4":case"latin4":return"ISO-8859-4";case"csisolatincyrillic":case"cyrillic":case"iso-8859-5":case"iso-ir-144":case"iso8859-5":case"iso88595":case"iso_8859-5":case"iso_8859-5:1988":return"ISO-8859-5";case"arabic":case"asmo-708":case"csiso88596e":case"csiso88596i":case"csisolatinarabic":case"ecma-114":case"iso-8859-6":case"iso-8859-6-e":case"iso-8859-6-i":case"iso-ir-127":case"iso8859-6":case"iso88596":case"iso_8859-6":case"iso_8859-6:1987":return"ISO-8859-6";case"csisolatingreek":case"ecma-118":case"elot_928":case"greek":case"greek8":case"iso-8859-7":case"iso-ir-126":case"iso8859-7":case"iso88597":case"iso_8859-7":case"iso_8859-7:1987":case"sun_eu_greek":return"ISO-8859-7";case"csiso88598e":case"csisolatinhebrew":case"hebrew":case"iso-8859-8":case"iso-8859-8-e":case"iso-ir-138":case"iso8859-8":case"iso88598":case"iso_8859-8":case"iso_8859-8:1988":case"visual":return"ISO-8859-8";case"csiso88598i":case"iso-8859-8-i":case"logical":return"ISO-8859-8-I";case"csisolatin6":case"iso-8859-10":case"iso-ir-157":case"iso8859-10":case"iso885910":case"l6":case"latin6":return"ISO-8859-10";case"iso-8859-13":case"iso8859-13":case"iso885913":return"ISO-8859-13";case"iso-8859-14":case"iso8859-14":case"iso885914":return"ISO-8859-14";case"csisolatin9":case"iso-8859-15":case"iso8859-15":case"iso885915":case"iso_8859-15":case"l9":return"ISO-8859-15";case"iso-8859-16":return"ISO-8859-16";case"cskoi8r":case"koi":case"koi8":case"koi8-r":case"koi8_r":return"KOI8-R";case"koi8-ru":case"koi8-u":return"KOI8-U";case"csmacintosh":case"mac":case"macintosh":case"x-mac-roman":return"macintosh";case"iso-8859-11":case"iso8859-11":case"iso885911":case"tis-620":case"windows-874":return"windows-874";case"cp1250":case"windows-1250":case"x-cp1250":return"windows-1250";case"cp1251":case"windows-1251":case"x-cp1251":return"windows-1251";case"ansi_x3.4-1968":case"ascii":case"cp1252":case"cp819":case"csisolatin1":case"ibm819":case"iso-8859-1":case"iso-ir-100":case"iso8859-1":case"iso88591":case"iso_8859-1":case"iso_8859-1:1987":case"l1":case"latin1":case"us-ascii":case"windows-1252":case"x-cp1252":return"windows-1252";case"cp1253":case"windows-1253":case"x-cp1253":return"windows-1253";case"cp1254":case"csisolatin5":case"iso-8859-9":case"iso-ir-148":case"iso8859-9":case"iso88599":case"iso_8859-9":case"iso_8859-9:1989":case"l5":case"latin5":case"windows-1254":case"x-cp1254":return"windows-1254";case"cp1255":case"windows-1255":case"x-cp1255":return"windows-1255";case"cp1256":case"windows-1256":case"x-cp1256":return"windows-1256";case"cp1257":case"windows-1257":case"x-cp1257":return"windows-1257";case"cp1258":case"windows-1258":case"x-cp1258":return"windows-1258";case"x-mac-cyrillic":case"x-mac-ukrainian":return"x-mac-cyrillic";case"chinese":case"csgb2312":case"csiso58gb231280":case"gb2312":case"gb_2312":case"gb_2312-80":case"gbk":case"iso-ir-58":case"x-gbk":return"GBK";case"gb18030":return"gb18030";case"big5":case"big5-hkscs":case"cn-big5":case"csbig5":case"x-x-big5":return"Big5";case"cseucpkdfmtjapanese":case"euc-jp":case"x-euc-jp":return"EUC-JP";case"csiso2022jp":case"iso-2022-jp":return"ISO-2022-JP";case"csshiftjis":case"ms932":case"ms_kanji":case"shift-jis":case"shift_jis":case"sjis":case"windows-31j":case"x-sjis":return"Shift_JIS";case"cseuckr":case"csksc56011987":case"euc-kr":case"iso-ir-149":case"korean":case"ks_c_5601-1987":case"ks_c_5601-1989":case"ksc5601":case"ksc_5601":case"windows-949":return"EUC-KR";case"csiso2022kr":case"hz-gb-2312":case"iso-2022-cn":case"iso-2022-cn-ext":case"iso-2022-kr":case"replacement":return"replacement";case"unicodefffe":case"utf-16be":return"UTF-16BE";case"csunicode":case"iso-10646-ucs-2":case"ucs-2":case"unicode":case"unicodefeff":case"utf-16":case"utf-16le":return"UTF-16LE";case"x-user-defined":return"x-user-defined";default:return"failure"}}}),{serializeAMimeType:i,parseMIMEType:a}=Se(),{types:g}=l,{StringDecoder:c}=S,{btoa:Q}=C;
/**
	 * @see https://w3c.github.io/FileAPI/#fire-a-progress-event
	 * @see https://dom.spec.whatwg.org/#concept-event-fire
	 * @param {string} e The name of the event
	 * @param {import('./filereader').FileReader} reader
	 */
function E(A,e){
// The progress event e does not bubble. e.bubbles must be false
// The progress event e is NOT cancelable. e.cancelable must be false
const t=new s(A,{bubbles:!1,cancelable:!1});e.dispatchEvent(t)}
/**
	 * @see https://w3c.github.io/FileAPI/#blob-package-data
	 * @param {Uint8Array[]} bytes
	 * @param {string} type
	 * @param {string?} mimeType
	 * @param {string?} encodingName
	 */function B(A,e,t,r){
// 1. A Blob has an associated package data algorithm, given
//    bytes, a type, a optional mimeType, and a optional
//    encodingName, which switches on type and runs the
//    associated steps:
switch(e){case"DataURL":{
// 1. Return bytes as a DataURL [RFC2397] subject to
//    the considerations below:
//  * Use mimeType as part of the Data URL if it is
//    available in keeping with the Data URL
//    specification [RFC2397].
//  * If mimeType is not available return a Data URL
//    without a media-type. [RFC2397].
// https://datatracker.ietf.org/doc/html/rfc2397#section-3
// dataurl    := "data:" [ mediatype ] [ ";base64" ] "," data
// mediatype  := [ type "/" subtype ] *( ";" parameter )
// data       := *urlchar
// parameter  := attribute "=" value
let e="data:";const r=a(t||"application/octet-stream");"failure"!==r&&(e+=i(r)),e+=";base64,";const n=new c("latin1");for(const t of A)e+=Q(n.write(t));return e+=Q(n.end()),e}case"Text":{
// 1. Let encoding be failure
let e="failure";
// 2. If the encodingName is present, set encoding to the
//    result of getting an encoding from encodingName.
// 3. If encoding is failure, and mimeType is present:
if(r&&(e=o(r)),"failure"===e&&t){
// 1. Let type be the result of parse a MIME type
//    given mimeType.
const A=a(t);
// 2. If type is not failure, set encoding to the result
//    of getting an encoding from type’s parameters["charset"].
"failure"!==A&&(e=o(A.parameters.get("charset")))}
// 4. If encoding is failure, then set encoding to UTF-8.
// 5. Decode bytes using fallback encoding encoding, and
//    return the result.
return"failure"===e&&(e="UTF-8"),
/**
	 * @see https://encoding.spec.whatwg.org/#decode
	 * @param {Uint8Array[]} ioQueue
	 * @param {string} encoding
	 */
function(A,e){const t=h(A),r=
/**
	 * @see https://encoding.spec.whatwg.org/#bom-sniff
	 * @param {Uint8Array} ioQueue
	 */
function(A){
// 1. Let BOM be the result of peeking 3 bytes from ioQueue,
//    converted to a byte sequence.
const[e,t,r]=A;
// 2. For each of the rows in the table below, starting with
//    the first one and going down, if BOM starts with the
//    bytes given in the first column, then return the
//    encoding given in the cell in the second column of that
//    row. Otherwise, return null.
if(239===e&&187===t&&191===r)return"UTF-8";if(254===e&&255===t)return"UTF-16BE";if(255===e&&254===t)return"UTF-16LE";return null}
/**
	 * @param {Uint8Array[]} sequences
	 */(t);
// 1. Let BOMEncoding be the result of BOM sniffing ioQueue.
let n=0;
// 2. If BOMEncoding is non-null:
null!==r&&(
// 1. Set encoding to BOMEncoding.
e=r,
// 2. Read three bytes from ioQueue, if BOMEncoding is
//    UTF-8; otherwise read two bytes.
//    (Do nothing with those bytes.)
n="UTF-8"===r?3:2);
// 3. Process a queue with an instance of encoding’s
//    decoder, ioQueue, output, and "replacement".
// 4. Return output.
const s=t.slice(n);return new TextDecoder(e).decode(s)}(A,e)}case"ArrayBuffer":return h(A).buffer;case"BinaryString":{
// Return bytes as a binary string, in which every byte
//  is represented by a code unit of equal value [0..255].
let e="";const t=new c("latin1");for(const r of A)e+=t.write(r);return e+=t.end(),e}}}function h(A){const e=A.reduce((A,e)=>A+e.byteLength,0);let t=0;return A.reduce((A,e)=>(A.set(e,t),t+=e.byteLength,A),new Uint8Array(e))}return cr={staticPropertyDescriptors:{enumerable:!0,writable:!1,configurable:!1},readOperation:
/**
	 * @see https://w3c.github.io/FileAPI/#readOperation
	 * @param {import('./filereader').FileReader} fr
	 * @param {import('buffer').Blob} blob
	 * @param {string} type
	 * @param {string?} encodingName
	 */
function(s,o,i,a){
// 1. If fr’s state is "loading", throw an InvalidStateError
//    DOMException.
if("loading"===s[A])throw new DOMException("Invalid state","InvalidStateError");
// 2. Set fr’s state to "loading".
s[A]="loading",
// 3. Set fr’s result to null.
s[t]=null,
// 4. Set fr’s error to null.
s[e]=null;
// 5. Let stream be the result of calling get stream on blob.
/** @type {import('stream/web').ReadableStream} */
const c=o.stream().getReader(),Q=[];
// 6. Let reader be the result of getting a reader from stream.
// 8. Let chunkPromise be the result of reading a chunk from
//    stream with reader.
let h=c.read(),I=!0;
// 9. Let isFirstChunk be true.
(async()=>{for(;!s[r];)
// 1. Wait for chunkPromise to be fulfilled or rejected.
try{const{done:C,value:l}=await h;
// 2. If chunkPromise is fulfilled, and isFirstChunk is
//    true, queue a task to fire a progress event called
//    loadstart at fr.
// 4. If chunkPromise is fulfilled with an object whose
//    done property is false and whose value property is
//    a Uint8Array object, run these steps:
if(I&&!s[r]&&queueMicrotask(()=>{E("loadstart",s)}),
// 3. Set isFirstChunk to false.
I=!1,!C&&g.isUint8Array(l))
// 1. Let bs be the byte sequence represented by the
//    Uint8Array object.
// 2. Append bs to bytes.
Q.push(l),
// 3. If roughly 50ms have passed since these steps
//    were last invoked, queue a task to fire a
//    progress event called progress at fr.
(void 0===s[n]||Date.now()-s[n]>=50)&&!s[r]&&(s[n]=Date.now(),queueMicrotask(()=>{E("progress",s)})),
// 4. Set chunkPromise to the result of reading a
//    chunk from stream with reader.
h=c.read();else if(C){
// 5. Otherwise, if chunkPromise is fulfilled with an
//    object whose done property is true, queue a task
//    to run the following steps and abort this algorithm:
queueMicrotask(()=>{
// 1. Set fr’s state to "done".
s[A]="done";
// 2. Let result be the result of package data given
//    bytes, type, blob’s type, and encodingName.
try{const A=B(Q,i,o.type,a);
// 4. Else:
if(s[r])return;
// 1. Set fr’s result to result.
s[t]=A,
// 2. Fire a progress event called load at the fr.
E("load",s)}catch(A){
// 3. If package data threw an exception error:
// 1. Set fr’s error to error.
s[e]=A,
// 2. Fire a progress event called error at fr.
E("error",s)}
// 5. If fr’s state is not "loading", fire a progress
//    event called loadend at the fr.
"loading"!==s[A]&&E("loadend",s)});break}}catch(t){if(s[r])return;
// 6. Otherwise, if chunkPromise is rejected with an
//    error error, queue a task to run the following
//    steps and abort this algorithm:
queueMicrotask(()=>{
// 1. Set fr’s state to "done".
s[A]="done",
// 2. Set fr’s error to error.
s[e]=t,
// 3. Fire a progress event called error at fr.
E("error",s),
// 4. If fr’s state is not "loading", fire a progress
//    event called loadend at fr.
"loading"!==s[A]&&E("loadend",s)});break}})()},fireAProgressEvent:E},cr}function mn(){return Ir?hr:(Ir=1,hr={kConstruct:BA().kConstruct})}function kn(){if(dr)return ur;dr=1;const{kConstruct:A}=mn(),{urlEquals:e,getFieldValues:t}=function(){if(lr)return Cr;lr=1;const A=E,{URLSerializer:e}=Se(),{isValidHeaderName:t}=Me();return Cr={urlEquals:
/**
	 * @see https://url.spec.whatwg.org/#concept-url-equals
	 * @param {URL} A
	 * @param {URL} B
	 * @param {boolean | undefined} excludeFragment
	 * @returns {boolean}
	 */
function(A,t,r=!1){return e(A,r)===e(t,r)}
/**
	 * @see https://github.com/chromium/chromium/blob/694d20d134cb553d8d89e5500b9148012b1ba299/content/browser/cache_storage/cache_storage_cache.cc#L260-L262
	 * @param {string} header
	 */,getFieldValues:function(e){A(null!==e);const r=[];for(let A of e.split(","))A=A.trim(),t(A)&&r.push(A);return r}}}(),{kEnumerableProperty:r,isDisturbed:n}=CA(),{webidl:s}=Ne(),{Response:o,cloneResponse:i,fromInnerResponse:a}=dn(),{Request:g,fromInnerRequest:c}=pn(),{kState:Q}=Ue(),{fetching:B}=wn(),{urlIsHttpHttpsScheme:h,createDeferredPromise:I,readAllBytes:C}=Me(),l=E;
/**
	 * @see https://w3c.github.io/ServiceWorker/#dfn-cache-batch-operation
	 * @typedef {Object} CacheBatchOperation
	 * @property {'delete' | 'put'} type
	 * @property {any} request
	 * @property {any} response
	 * @property {import('../../types/cache').CacheQueryOptions} options
	 */
/**
	 * @see https://w3c.github.io/ServiceWorker/#dfn-request-response-list
	 * @typedef {[any, any][]} requestResponseList
	 */
class u{
/**
	   * @see https://w3c.github.io/ServiceWorker/#dfn-relevant-request-response-list
	   * @type {requestResponseList}
	   */
#b;constructor(){arguments[0]!==A&&s.illegalConstructor(),s.util.markAsUncloneable(this),this.#b=arguments[1]}async match(A,e={}){s.brandCheck(this,u);const t="Cache.match";s.argumentLengthCheck(arguments,1,t),A=s.converters.RequestInfo(A,t,"request"),e=s.converters.CacheQueryOptions(e,t,"options");const r=this.#F(A,e,1);if(0!==r.length)return r[0]}async matchAll(A=void 0,e={}){s.brandCheck(this,u);const t="Cache.matchAll";return void 0!==A&&(A=s.converters.RequestInfo(A,t,"request")),e=s.converters.CacheQueryOptions(e,t,"options"),this.#F(A,e)}async add(A){s.brandCheck(this,u);const e="Cache.add";s.argumentLengthCheck(arguments,1,e);
// 1.
const t=[A=s.converters.RequestInfo(A,e,"request")],r=this.addAll(t);
// 2.
// 3.
return await r}async addAll(A){s.brandCheck(this,u);const e="Cache.addAll";s.argumentLengthCheck(arguments,1,e);
// 1.
const r=[],n=[];
// 2.
// 3.
for(let t of A){if(void 0===t)throw s.errors.conversionFailed({prefix:e,argument:"Argument 1",types:["undefined is not allowed"]});if(t=s.converters.RequestInfo(t),"string"==typeof t)continue;
// 3.1
const A=t[Q];
// 3.2
if(!h(A.url)||"GET"!==A.method)throw s.errors.exception({header:e,message:"Expected http/s scheme when method is not GET."})}
// 4.
/** @type {ReturnType<typeof fetching>[]} */const o=[];
// 5.
for(const i of A){
// 5.1
const A=new g(i)[Q];
// 5.2
if(!h(A.url))throw s.errors.exception({header:e,message:"Expected http/s scheme."});
// 5.4
A.initiator="fetch",A.destination="subresource",
// 5.5
n.push(A);
// 5.6
const a=I();
// 5.7
o.push(B({request:A,processResponse(A){
// 1.
if("error"===A.type||206===A.status||A.status<200||A.status>299)a.reject(s.errors.exception({header:"Cache.addAll",message:"Received an invalid status code or the request failed."}));else if(A.headersList.contains("vary")){// 2.
// 2.1
const e=t(A.headersList.get("vary"));
// 2.2
for(const A of e)
// 2.2.1
if("*"===A){a.reject(s.errors.exception({header:"Cache.addAll",message:"invalid vary field value"}));for(const A of o)A.abort();return}}},processResponseEndOfBody(A){
// 1.
A.aborted?a.reject(new DOMException("aborted","AbortError")):
// 2.
a.resolve(A)}})),
// 5.8
r.push(a.promise)}
// 6.
const i=Promise.all(r),a=await i,c=[];
// 7.
// 7.2
let E=0;
// 7.3
for(const A of a){
// 7.3.1
/** @type {CacheBatchOperation} */
const e={type:"put",// 7.3.2
request:n[E],// 7.3.3
response:A};c.push(e),// 7.3.5
E++}
// 7.5
const C=I();
// 7.6.1
let l=null;
// 7.6.2
try{this.#S(c)}catch(A){l=A}
// 7.6.3
// 7.7
return queueMicrotask(()=>{
// 7.6.3.1
null===l?C.resolve(void 0):
// 7.6.3.2
C.reject(l)}),C.promise}async put(A,e){s.brandCheck(this,u);const r="Cache.put";s.argumentLengthCheck(arguments,2,r),A=s.converters.RequestInfo(A,r,"request"),e=s.converters.Response(e,r,"response");
// 1.
let o=null;
// 2.
// 4.
if(o=A instanceof g?A[Q]:new g(A)[Q],!h(o.url)||"GET"!==o.method)throw s.errors.exception({header:r,message:"Expected an http/s scheme when method is not GET"});
// 5.
const a=e[Q];
// 6.
if(206===a.status)throw s.errors.exception({header:r,message:"Got 206 status"});
// 7.
if(a.headersList.contains("vary")){
// 7.1.
const A=t(a.headersList.get("vary"));
// 7.2.
for(const e of A)
// 7.2.1
if("*"===e)throw s.errors.exception({header:r,message:"Got * vary field value"})}
// 8.
if(a.body&&(n(a.body.stream)||a.body.stream.locked))throw s.errors.exception({header:r,message:"Response body is locked or disturbed"});
// 9.
const c=i(a),E=I();
// 10.
// 11.
if(null!=a.body){
// 11.1
const A=a.body.stream.getReader();
// 11.2
// 11.3
C(A).then(E.resolve,E.reject)}else E.resolve(void 0);
// 12.
/** @type {CacheBatchOperation[]} */const B=[],l={type:"put",// 14.
request:o,// 15.
response:c};
// 13.
/** @type {CacheBatchOperation} */
// 17.
B.push(l);
// 19.
const d=await E.promise;null!=c.body&&(c.body.source=d);
// 19.1
const f=I();
// 19.2.1
let p=null;
// 19.2.2
try{this.#S(B)}catch(A){p=A}
// 19.2.3
return queueMicrotask(()=>{
// 19.2.3.1
null===p?f.resolve():// 19.2.3.2
f.reject(p)}),f.promise}async delete(A,e={}){s.brandCheck(this,u);const t="Cache.delete";s.argumentLengthCheck(arguments,1,t),A=s.converters.RequestInfo(A,t,"request"),e=s.converters.CacheQueryOptions(e,t,"options");
/**
	     * @type {Request}
	     */
let r=null;if(A instanceof g){if(r=A[Q],"GET"!==r.method&&!e.ignoreMethod)return!1}else l("string"==typeof A),r=new g(A)[Q];
/** @type {CacheBatchOperation[]} */const n=[],o={type:"delete",request:r,options:e};
/** @type {CacheBatchOperation} */n.push(o);const i=I();let a,c=null;try{a=this.#S(n)}catch(A){c=A}return queueMicrotask(()=>{null===c?i.resolve(!!a?.length):i.reject(c)}),i.promise}
/**
	   * @see https://w3c.github.io/ServiceWorker/#dom-cache-keys
	   * @param {any} request
	   * @param {import('../../types/cache').CacheQueryOptions} options
	   * @returns {Promise<readonly Request[]>}
	   */async keys(A=void 0,e={}){s.brandCheck(this,u);const t="Cache.keys";void 0!==A&&(A=s.converters.RequestInfo(A,t,"request")),e=s.converters.CacheQueryOptions(e,t,"options");
// 1.
let r=null;
// 2.
if(void 0!==A)
// 2.1
if(A instanceof g){
// 2.1.2
if(
// 2.1.1
r=A[Q],"GET"!==r.method&&!e.ignoreMethod)return[]}else"string"==typeof A&&(// 2.2
r=new g(A)[Q]);
// 4.
const n=I(),o=[];
// 5.
// 5.1
// 5.2
if(void 0===A)
// 5.2.1
for(const A of this.#b)
// 5.2.1.1
o.push(A[0]);else{// 5.3
// 5.3.1
const A=this.#N(r,e);
// 5.3.2
for(const e of A)
// 5.3.2.1
o.push(e[0])}
// 5.4
return queueMicrotask(()=>{
// 5.4.1
const A=[];
// 5.4.2
for(const e of o){const t=c(e,(new AbortController).signal,"immutable");
// 5.4.2.1
A.push(t)}
// 5.4.3
n.resolve(Object.freeze(A))}),n.promise}
/**
	   * @see https://w3c.github.io/ServiceWorker/#batch-cache-operations-algorithm
	   * @param {CacheBatchOperation[]} operations
	   * @returns {requestResponseList}
	   */#S(A){
// 1.
const e=this.#b,t=[...e],r=[],n=[];
// 2.
try{
// 4.2
for(const t of A){
// 4.2.1
if("delete"!==t.type&&"put"!==t.type)throw s.errors.exception({header:"Cache.#batchCacheOperations",message:'operation type does not match "delete" or "put"'});
// 4.2.2
if("delete"===t.type&&null!=t.response)throw s.errors.exception({header:"Cache.#batchCacheOperations",message:"delete operation should not have an associated response"});
// 4.2.3
if(this.#N(t.request,t.options,r).length)throw new DOMException("???","InvalidStateError");
// 4.2.4
let A;
// 4.2.5
if("delete"===t.type){
// TODO: the spec is wrong, this is needed to pass WPTs
if(
// 4.2.5.1
A=this.#N(t.request,t.options),0===A.length)return[];
// 4.2.5.2
for(const t of A){const A=e.indexOf(t);l(-1!==A),
// 4.2.5.2.1
e.splice(A,1)}}else if("put"===t.type){// 4.2.6
// 4.2.6.1
if(null==t.response)throw s.errors.exception({header:"Cache.#batchCacheOperations",message:"put operation should have an associated response"});
// 4.2.6.2
const n=t.request;
// 4.2.6.3
if(!h(n.url))throw s.errors.exception({header:"Cache.#batchCacheOperations",message:"expected http or https scheme"});
// 4.2.6.4
if("GET"!==n.method)throw s.errors.exception({header:"Cache.#batchCacheOperations",message:"not get method"});
// 4.2.6.5
if(null!=t.options)throw s.errors.exception({header:"Cache.#batchCacheOperations",message:"options must not be defined"});
// 4.2.6.6
A=this.#N(t.request);
// 4.2.6.7
for(const t of A){const A=e.indexOf(t);l(-1!==A),
// 4.2.6.7.1
e.splice(A,1)}
// 4.2.6.8
e.push([t.request,t.response]),
// 4.2.6.10
r.push([t.request,t.response])}
// 4.2.7
n.push([t.request,t.response])}
// 4.3
return n}catch(A){
// 5.3
// 5.
// 5.1
throw this.#b.length=0,
// 5.2
this.#b=t,A}}
/**
	   * @see https://w3c.github.io/ServiceWorker/#query-cache
	   * @param {any} requestQuery
	   * @param {import('../../types/cache').CacheQueryOptions} options
	   * @param {requestResponseList} targetStorage
	   * @returns {requestResponseList}
	   */#N(A,e,t){
/** @type {requestResponseList} */
const r=[],n=t??this.#b;for(const t of n){const[n,s]=t;this.#M(A,n,s,e)&&r.push(t)}return r}
/**
	   * @see https://w3c.github.io/ServiceWorker/#request-matches-cached-item-algorithm
	   * @param {any} requestQuery
	   * @param {any} request
	   * @param {any | null} response
	   * @param {import('../../types/cache').CacheQueryOptions | undefined} options
	   * @returns {boolean}
	   */#M(A,r,n=null,s){
// if (options?.ignoreMethod === false && request.method === 'GET') {
//   return false
// }
const o=new URL(A.url),i=new URL(r.url);if(s?.ignoreSearch&&(i.search="",o.search=""),!e(o,i,!0))return!1;if(null==n||s?.ignoreVary||!n.headersList.contains("vary"))return!0;const a=t(n.headersList.get("vary"));for(const e of a){if("*"===e)return!1;
// If one has the header and the other doesn't, or one has
// a different value than the other, return false
if(r.headersList.get(e)!==A.headersList.get(e))return!1}return!0}#F(A,e,t=1/0){
// 1.
let r=null;
// 2.
if(void 0!==A)if(A instanceof g){
// 2.1.2
if(
// 2.1.1
r=A[Q],"GET"!==r.method&&!e.ignoreMethod)return[]}else"string"==typeof A&&(
// 2.2.1
r=new g(A)[Q]);
// 5.
// 5.1
const n=[];
// 5.2
if(void 0===A)
// 5.2.1
for(const A of this.#b)n.push(A[1]);else{// 5.3
// 5.3.1
const A=this.#N(r,e);
// 5.3.2
for(const e of A)n.push(e[1])}
// 5.4
// We don't implement CORs so we don't need to loop over the responses, yay!
// 5.5.1
const s=[];
// 5.5.2
for(const A of n){
// 5.5.2.1
const e=a(A,"immutable");if(s.push(e.clone()),s.length>=t)break}
// 6.
return Object.freeze(s)}}Object.defineProperties(u.prototype,{[Symbol.toStringTag]:{value:"Cache",configurable:!0},match:r,matchAll:r,add:r,addAll:r,put:r,delete:r,keys:r});const d=[{key:"ignoreSearch",converter:s.converters.boolean,defaultValue:()=>!1},{key:"ignoreMethod",converter:s.converters.boolean,defaultValue:()=>!1},{key:"ignoreVary",converter:s.converters.boolean,defaultValue:()=>!1}];return s.converters.CacheQueryOptions=s.dictionaryConverter(d),s.converters.MultiCacheQueryOptions=s.dictionaryConverter([...d,{key:"cacheName",converter:s.converters.DOMString}]),s.converters.Response=s.interfaceConverter(o),s.converters["sequence<RequestInfo>"]=s.sequenceConverter(s.converters.RequestInfo),ur={Cache:u}}function Rn(){if(mr)return Dr;
/**
	 CHAR           = <any US-ASCII character (octets 0 - 127)>
	 token          = 1*<any CHAR except CTLs or separators>
	 separators     = "(" | ")" | "<" | ">" | "@"
	                | "," | ";" | ":" | "\" | <">
	                | "/" | "[" | "]" | "?" | "="
	                | "{" | "}" | SP | HT
	 * @param {string} name
	 */
function A(A){for(let e=0;e<A.length;++e){const t=A.charCodeAt(e);if(t<33||// exclude CTLs (0-31), SP and HT
t>126||// exclude non-ascii and DEL
34===t||// "
40===t||// (
41===t||// )
60===t||// <
62===t||// >
64===t||// @
44===t||// ,
59===t||// ;
58===t||// :
92===t||// \
47===t||// /
91===t||// [
93===t||// ]
63===t||// ?
61===t||// =
123===t||// {
125===t)throw new Error("Invalid cookie name")}}
/**
	 cookie-value      = *cookie-octet / ( DQUOTE *cookie-octet DQUOTE )
	 cookie-octet      = %x21 / %x23-2B / %x2D-3A / %x3C-5B / %x5D-7E
	                       ; US-ASCII characters excluding CTLs,
	                       ; whitespace DQUOTE, comma, semicolon,
	                       ; and backslash
	 * @param {string} value
	 */function e(A){let e=A.length,t=0;
// if the value is wrapped in DQUOTE
if('"'===A[0]){if(1===e||'"'!==A[e-1])throw new Error("Invalid cookie value");--e,++t}for(;t<e;){const e=A.charCodeAt(t++);if(e<33||// exclude CTLs (0-31)
e>126||// non-ascii and DEL (127)
34===e||// "
44===e||// ,
59===e||// ;
92===e)throw new Error("Invalid cookie value")}}
/**
	 * path-value        = <any CHAR except CTLs or ";">
	 * @param {string} path
	 */function t(A){for(let e=0;e<A.length;++e){const t=A.charCodeAt(e);if(t<32||// exclude CTLs (0-31)
t>126||// exclude DEL and non-ascii
59===t)throw new Error("Invalid cookie path")}}
/**
	 * <let-dig> ::= <letter> | <digit>
	 *
	 * <letter> ::= any one of the 52 alphabetic characters A through Z in
	 * upper case and a through z in lower case
	 *
	 * <digit> ::= any one of the ten digits 0 through 9r
	 *
	 * @see https://www.rfc-editor.org/rfc/rfc1034#section-3.5
	 * @param {number} code
	 */function r(A){return A>=48&&A<=57||// 0-9
A>=65&&A<=90||// A-Z
A>=97&&A<=122;// a-z
}
/**
	 * Validates a cookie domain against the "preferred name syntax".
	 *
	 * <domain>      ::= <subdomain> | " "
	 * <subdomain>   ::= <label> | <subdomain> "." <label>
	 * <label>       ::= <let-dig> [ [ <ldh-str> ] <let-dig> ]
	 * <ldh-str>     ::= <let-dig-hyp> | <let-dig-hyp> <ldh-str>
	 * <let-dig-hyp> ::= <let-dig> | "-"
	 *
	 * @see https://www.rfc-editor.org/rfc/rfc1034#section-3.5
	 * @see https://www.rfc-editor.org/rfc/rfc1123#section-2.1
	 * @see https://www.rfc-editor.org/rfc/rfc1035#section-2.3.4
	 * @param {string} domain
	 */mr=1;const n=["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],s=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],o=Array(61).fill(0).map((A,e)=>e.toString().padStart(2,"0"));
/**
	 * @see https://www.rfc-editor.org/rfc/rfc7231#section-7.1.1.1
	 * @param {number|Date} date
	  IMF-fixdate  = day-name "," SP date1 SP time-of-day SP GMT
	  ; fixed length/zone/capitalization subset of the format
	  ; see Section 3.3 of [RFC5322]

	  day-name     = %x4D.6F.6E ; "Mon", case-sensitive
	              / %x54.75.65 ; "Tue", case-sensitive
	              / %x57.65.64 ; "Wed", case-sensitive
	              / %x54.68.75 ; "Thu", case-sensitive
	              / %x46.72.69 ; "Fri", case-sensitive
	              / %x53.61.74 ; "Sat", case-sensitive
	              / %x53.75.6E ; "Sun", case-sensitive
	  date1        = day SP month SP year
	                  ; e.g., 02 Jun 1982

	  day          = 2DIGIT
	  month        = %x4A.61.6E ; "Jan", case-sensitive
	              / %x46.65.62 ; "Feb", case-sensitive
	              / %x4D.61.72 ; "Mar", case-sensitive
	              / %x41.70.72 ; "Apr", case-sensitive
	              / %x4D.61.79 ; "May", case-sensitive
	              / %x4A.75.6E ; "Jun", case-sensitive
	              / %x4A.75.6C ; "Jul", case-sensitive
	              / %x41.75.67 ; "Aug", case-sensitive
	              / %x53.65.70 ; "Sep", case-sensitive
	              / %x4F.63.74 ; "Oct", case-sensitive
	              / %x4E.6F.76 ; "Nov", case-sensitive
	              / %x44.65.63 ; "Dec", case-sensitive
	  year         = 4DIGIT

	  GMT          = %x47.4D.54 ; "GMT", case-sensitive

	  time-of-day  = hour ":" minute ":" second
	              ; 00:00:00 - 23:59:60 (leap second)

	  hour         = 2DIGIT
	  minute       = 2DIGIT
	  second       = 2DIGIT
	 */
function i(A){return"number"==typeof A&&(A=new Date(A)),`${n[A.getUTCDay()]}, ${o[A.getUTCDate()]} ${s[A.getUTCMonth()]} ${A.getUTCFullYear()} ${o[A.getUTCHours()]}:${o[A.getUTCMinutes()]}:${o[A.getUTCSeconds()]} GMT`
/**
	 max-age-av        = "Max-Age=" non-zero-digit *DIGIT
	                       ; In practice, both expires-av and max-age-av
	                       ; are limited to dates representable by the
	                       ; user agent.
	 * @param {number} maxAge
	 */}return Dr={isCTLExcludingHtab:
/**
	 * @param {string} value
	 * @returns {boolean}
	 */
function(A){for(let e=0;e<A.length;++e){const t=A.charCodeAt(e);if(t>=0&&t<=8||t>=10&&t<=31||127===t)return!0}return!1},validateCookieName:A,validateCookiePath:t,validateCookieValue:e,toIMFDate:i,stringify:
/**
	 * @see https://www.rfc-editor.org/rfc/rfc6265#section-4.1.1
	 * @param {import('./index').Cookie} cookie
	 */
function(n){if(0===n.name.length)return null;A(n.name),e(n.value);const s=[`${n.name}=${n.value}`];
// https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-cookie-prefixes-00#section-3.1
// https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-cookie-prefixes-00#section-3.2
n.name.startsWith("__Secure-")&&(n.secure=!0),n.name.startsWith("__Host-")&&(n.secure=!0,n.domain=null,n.path="/"),n.secure&&s.push("Secure"),n.httpOnly&&s.push("HttpOnly"),"number"==typeof n.maxAge&&(!function(A){if(A<0)throw new Error("Invalid cookie max-age")}(n.maxAge),s.push(`Max-Age=${n.maxAge}`)),n.domain&&(!function(A){
// <domain> ::= <subdomain> | " "
if(" "===A)return;if(A.length>255)throw new Error("Invalid cookie domain");let e=0;for(let t=0;t<A.length;++t){const n=A.charCodeAt(t);if(46!==n){if(0===e&&!r(n))throw new Error("Invalid cookie domain");if(!r(n)&&45!==n)// "-"
throw new Error("Invalid cookie domain");if(++e>63)throw new Error("Invalid cookie domain")}else{if(0===e)throw new Error("Invalid cookie domain");if(45===A.charCodeAt(t-1))// "-"
throw new Error("Invalid cookie domain");e=0}}if(0===e||45===A.charCodeAt(A.length-1))// "-"
throw new Error("Invalid cookie domain")}(n.domain),s.push(`Domain=${n.domain}`)),n.path&&(t(n.path),s.push(`Path=${n.path}`)),n.expires&&"Invalid Date"!==n.expires.toString()&&s.push(`Expires=${i(n.expires)}`),n.sameSite&&s.push(`SameSite=${n.sameSite}`);for(const t of n.unparsed){if(!t.includes("="))throw new Error("Invalid unparsed");const[r,...n]=t.split("="),o=r.trim(),i=n.join("=");A(o),e(i),s.push(`${o}=${i}`)}return s.join("; ")}}}function bn(){if(Rr)return kr;Rr=1;const{maxNameValuePairSize:A,maxAttributeValueSize:e}=yr?wr:(yr=1,wr={maxAttributeValueSize:1024,maxNameValuePairSize:4096}),{isCTLExcludingHtab:t}=Rn(),{collectASequenceOfCodePointsFast:r}=Se(),n=E;
/**
	 * Parses the remaining attributes of a set-cookie header
	 * @see https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-rfc6265bis#section-5.4
	 * @param {string} unparsedAttributes
	 * @param {[Object.<string, unknown>]={}} cookieAttributeList
	 */
function s(A,t={}){
// 1. If the unparsed-attributes string is empty, skip the rest of
//    these steps.
if(0===A.length)return t;
// 2. Discard the first character of the unparsed-attributes (which
//    will be a %x3B (";") character).
n(";"===A[0]);let o="";
// 3. If the remaining unparsed-attributes contains a %x3B (";")
//    character:
(A=A.slice(1)).includes(";")?(
// 1. Consume the characters of the unparsed-attributes up to, but
//    not including, the first %x3B (";") character.
o=r(";",A,{position:0}),A=A.slice(o.length)):(
// Otherwise:
// 1. Consume the remainder of the unparsed-attributes.
o=A,A="");
// Let the cookie-av string be the characters consumed in this step.
let i="",a="";
// 4. If the cookie-av string contains a %x3D ("=") character:
if(o.includes("=")){
// 1. The (possibly empty) attribute-name string consists of the
//    characters up to, but not including, the first %x3D ("=")
//    character, and the (possibly empty) attribute-value string
//    consists of the characters after the first %x3D ("=")
//    character.
const A={position:0};i=r("=",o,A),a=o.slice(A.position+1)}else
// Otherwise:
// 1. The attribute-name string consists of the entire cookie-av
//    string, and the attribute-value string is empty.
i=o;
// 5. Remove any leading or trailing WSP characters from the attribute-
//    name string and the attribute-value string.
// 6. If the attribute-value is longer than 1024 octets, ignore the
//    cookie-av string and return to Step 1 of this algorithm.
if(i=i.trim(),a=a.trim(),a.length>e)return s(A,t);
// 7. Process the attribute-name and attribute-value according to the
//    requirements in the following subsections.  (Notice that
//    attributes with unrecognized attribute-names are ignored.)
const g=i.toLowerCase();
// https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-rfc6265bis#section-5.4.1
// If the attribute-name case-insensitively matches the string
// "Expires", the user agent MUST process the cookie-av as follows.
if("expires"===g){
// 1. Let the expiry-time be the result of parsing the attribute-value
//    as cookie-date (see Section 5.1.1).
const A=new Date(a);
// 2. If the attribute-value failed to parse as a cookie date, ignore
//    the cookie-av.
t.expires=A}else if("max-age"===g){
// https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-rfc6265bis#section-5.4.2
// If the attribute-name case-insensitively matches the string "Max-
// Age", the user agent MUST process the cookie-av as follows.
// 1. If the first character of the attribute-value is not a DIGIT or a
//    "-" character, ignore the cookie-av.
const e=a.charCodeAt(0);if((e<48||e>57)&&"-"!==a[0])return s(A,t);
// 2. If the remainder of attribute-value contains a non-DIGIT
//    character, ignore the cookie-av.
if(!/^\d+$/.test(a))return s(A,t);
// 3. Let delta-seconds be the attribute-value converted to an integer.
const r=Number(a);
// 4. Let cookie-age-limit be the maximum age of the cookie (which
//    SHOULD be 400 days or less, see Section 4.1.2.2).
// 5. Set delta-seconds to the smaller of its present value and cookie-
//    age-limit.
// deltaSeconds = Math.min(deltaSeconds * 1000, maxExpiresMs)
// 6. If delta-seconds is less than or equal to zero (0), let expiry-
//    time be the earliest representable date and time.  Otherwise, let
//    the expiry-time be the current date and time plus delta-seconds
//    seconds.
// const expiryTime = deltaSeconds <= 0 ? Date.now() : Date.now() + deltaSeconds
// 7. Append an attribute to the cookie-attribute-list with an
//    attribute-name of Max-Age and an attribute-value of expiry-time.
t.maxAge=r}else if("domain"===g){
// https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-rfc6265bis#section-5.4.3
// If the attribute-name case-insensitively matches the string "Domain",
// the user agent MUST process the cookie-av as follows.
// 1. Let cookie-domain be the attribute-value.
let A=a;
// 2. If cookie-domain starts with %x2E ("."), let cookie-domain be
//    cookie-domain without its leading %x2E (".").
"."===A[0]&&(A=A.slice(1)),
// 3. Convert the cookie-domain to lower case.
A=A.toLowerCase(),
// 4. Append an attribute to the cookie-attribute-list with an
//    attribute-name of Domain and an attribute-value of cookie-domain.
t.domain=A}else if("path"===g){
// https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-rfc6265bis#section-5.4.4
// If the attribute-name case-insensitively matches the string "Path",
// the user agent MUST process the cookie-av as follows.
// 1. If the attribute-value is empty or if the first character of the
//    attribute-value is not %x2F ("/"):
let A="";
// 1. Let cookie-path be the default-path.
A=0===a.length||"/"!==a[0]?"/":a,
// 2. Append an attribute to the cookie-attribute-list with an
//    attribute-name of Path and an attribute-value of cookie-path.
t.path=A}else if("secure"===g)
// https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-rfc6265bis#section-5.4.5
// If the attribute-name case-insensitively matches the string "Secure",
// the user agent MUST append an attribute to the cookie-attribute-list
// with an attribute-name of Secure and an empty attribute-value.
t.secure=!0;else if("httponly"===g)
// https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-rfc6265bis#section-5.4.6
// If the attribute-name case-insensitively matches the string
// "HttpOnly", the user agent MUST append an attribute to the cookie-
// attribute-list with an attribute-name of HttpOnly and an empty
// attribute-value.
t.httpOnly=!0;else if("samesite"===g){
// https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-rfc6265bis#section-5.4.7
// If the attribute-name case-insensitively matches the string
// "SameSite", the user agent MUST process the cookie-av as follows:
const A=a.toLowerCase();
// 1. If cookie-av's attribute-value is a case-insensitive match for
//    "None", append an attribute to the cookie-attribute-list with an
//    attribute-name of "SameSite" and an attribute-value of "None".
"none"===A?t.sameSite="None":"strict"===A?
// 2. If cookie-av's attribute-value is a case-insensitive match for
//    "Strict", append an attribute to the cookie-attribute-list with
//    an attribute-name of "SameSite" and an attribute-value of
//    "Strict".
t.sameSite="Strict":"lax"===A&&(
// 3. If cookie-av's attribute-value is a case-insensitive match for
//    "Lax", append an attribute to the cookie-attribute-list with an
//    attribute-name of "SameSite" and an attribute-value of "Lax".
t.sameSite="Lax")}else t.unparsed??=[],t.unparsed.push(`${i}=${a}`);
// 8. Return to Step 1 of this algorithm.
return s(A,t)}return kr={parseSetCookie:
/**
	 * @description Parses the field-value attributes of a set-cookie header string.
	 * @see https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-rfc6265bis#section-5.4
	 * @param {string} header
	 * @returns if the header is invalid, null will be returned
	 */
function(e){
// 1. If the set-cookie-string contains a %x00-08 / %x0A-1F / %x7F
//    character (CTL characters excluding HTAB): Abort these steps and
//    ignore the set-cookie-string entirely.
if(t(e))return null;let n="",o="",i="",a="";
// 2. If the set-cookie-string contains a %x3B (";") character:
if(e.includes(";")){
// 1. The name-value-pair string consists of the characters up to,
//    but not including, the first %x3B (";"), and the unparsed-
//    attributes consist of the remainder of the set-cookie-string
//    (including the %x3B (";") in question).
const A={position:0};n=r(";",e,A),o=e.slice(A.position)}else
// Otherwise:
// 1. The name-value-pair string consists of all the characters
//    contained in the set-cookie-string, and the unparsed-
//    attributes is the empty string.
n=e;
// 3. If the name-value-pair string lacks a %x3D ("=") character, then
//    the name string is empty, and the value string is the value of
//    name-value-pair.
if(n.includes("=")){
//    Otherwise, the name string consists of the characters up to, but
//    not including, the first %x3D ("=") character, and the (possibly
//    empty) value string consists of the characters after the first
//    %x3D ("=") character.
const A={position:0};i=r("=",n,A),a=n.slice(A.position+1)}
// 4. Remove any leading or trailing WSP characters from the name
//    string and the value string.
else a=n;
// 5. If the sum of the lengths of the name string and the value string
//    is more than 4096 octets, abort these steps and ignore the set-
//    cookie-string entirely.
return i=i.trim(),a=a.trim(),i.length+a.length>A?null:{name:i,value:a,...s(o)};
// 6. The cookie-name is the name string, and the cookie-value is the
//    value string.
},parseUnparsedAttributes:s}}function Fn(){if(Nr)return Sr;Nr=1;const{webidl:A}=Ne(),{kEnumerableProperty:e}=CA(),{kConstruct:t}=BA(),{MessagePort:r}=m;
/**
	 * @see https://html.spec.whatwg.org/multipage/comms.html#messageevent
	 */
class n extends Event{#U;constructor(e,r={}){if(e===t)return super(arguments[1],arguments[2]),void A.util.markAsUncloneable(this);const n="MessageEvent constructor";A.argumentLengthCheck(arguments,1,n),super(e=A.converters.DOMString(e,n,"type"),r=A.converters.MessageEventInit(r,n,"eventInitDict")),this.#U=r,A.util.markAsUncloneable(this)}get data(){return A.brandCheck(this,n),this.#U.data}get origin(){return A.brandCheck(this,n),this.#U.origin}get lastEventId(){return A.brandCheck(this,n),this.#U.lastEventId}get source(){return A.brandCheck(this,n),this.#U.source}get ports(){return A.brandCheck(this,n),Object.isFrozen(this.#U.ports)||Object.freeze(this.#U.ports),this.#U.ports}initMessageEvent(e,t=!1,r=!1,s=null,o="",i="",a=null,g=[]){return A.brandCheck(this,n),A.argumentLengthCheck(arguments,1,"MessageEvent.initMessageEvent"),new n(e,{bubbles:t,cancelable:r,data:s,origin:o,lastEventId:i,source:a,ports:g})}static createFastMessageEvent(A,e){const r=new n(t,A,e);return r.#U=e,r.#U.data??=null,r.#U.origin??="",r.#U.lastEventId??="",r.#U.source??=null,r.#U.ports??=[],r}}const{createFastMessageEvent:s}=n;delete n.createFastMessageEvent;
/**
	 * @see https://websockets.spec.whatwg.org/#the-closeevent-interface
	 */
class o extends Event{#U;constructor(e,t={}){const r="CloseEvent constructor";A.argumentLengthCheck(arguments,1,r),super(e=A.converters.DOMString(e,r,"type"),t=A.converters.CloseEventInit(t)),this.#U=t,A.util.markAsUncloneable(this)}get wasClean(){return A.brandCheck(this,o),this.#U.wasClean}get code(){return A.brandCheck(this,o),this.#U.code}get reason(){return A.brandCheck(this,o),this.#U.reason}}
// https://html.spec.whatwg.org/multipage/webappapis.html#the-errorevent-interface
class i extends Event{#U;constructor(e,t){const r="ErrorEvent constructor";A.argumentLengthCheck(arguments,1,r),super(e,t),A.util.markAsUncloneable(this),e=A.converters.DOMString(e,r,"type"),t=A.converters.ErrorEventInit(t??{}),this.#U=t}get message(){return A.brandCheck(this,i),this.#U.message}get filename(){return A.brandCheck(this,i),this.#U.filename}get lineno(){return A.brandCheck(this,i),this.#U.lineno}get colno(){return A.brandCheck(this,i),this.#U.colno}get error(){return A.brandCheck(this,i),this.#U.error}}Object.defineProperties(n.prototype,{[Symbol.toStringTag]:{value:"MessageEvent",configurable:!0},data:e,origin:e,lastEventId:e,source:e,ports:e,initMessageEvent:e}),Object.defineProperties(o.prototype,{[Symbol.toStringTag]:{value:"CloseEvent",configurable:!0},reason:e,code:e,wasClean:e}),Object.defineProperties(i.prototype,{[Symbol.toStringTag]:{value:"ErrorEvent",configurable:!0},message:e,filename:e,lineno:e,colno:e,error:e}),A.converters.MessagePort=A.interfaceConverter(r),A.converters["sequence<MessagePort>"]=A.sequenceConverter(A.converters.MessagePort);const a=[{key:"bubbles",converter:A.converters.boolean,defaultValue:()=>!1},{key:"cancelable",converter:A.converters.boolean,defaultValue:()=>!1},{key:"composed",converter:A.converters.boolean,defaultValue:()=>!1}];return A.converters.MessageEventInit=A.dictionaryConverter([...a,{key:"data",converter:A.converters.any,defaultValue:()=>null},{key:"origin",converter:A.converters.USVString,defaultValue:()=>""},{key:"lastEventId",converter:A.converters.DOMString,defaultValue:()=>""},{key:"source",
// Node doesn't implement WindowProxy or ServiceWorker, so the only
// valid value for source is a MessagePort.
converter:A.nullableConverter(A.converters.MessagePort),defaultValue:()=>null},{key:"ports",converter:A.converters["sequence<MessagePort>"],defaultValue:()=>new Array(0)}]),A.converters.CloseEventInit=A.dictionaryConverter([...a,{key:"wasClean",converter:A.converters.boolean,defaultValue:()=>!1},{key:"code",converter:A.converters["unsigned short"],defaultValue:()=>0},{key:"reason",converter:A.converters.USVString,defaultValue:()=>""}]),A.converters.ErrorEventInit=A.dictionaryConverter([...a,{key:"message",converter:A.converters.DOMString,defaultValue:()=>""},{key:"filename",converter:A.converters.USVString,defaultValue:()=>""},{key:"lineno",converter:A.converters["unsigned long"],defaultValue:()=>0},{key:"colno",converter:A.converters["unsigned long"],defaultValue:()=>0},{key:"error",converter:A.converters.any}]),Sr={MessageEvent:n,CloseEvent:o,ErrorEvent:i,createFastMessageEvent:s}}function Sn(){if(Ur)return Mr;Ur=1;
// This is a Globally Unique Identifier unique used
// to validate that the endpoint accepts websocket
// connections.
// See https://www.rfc-editor.org/rfc/rfc6455.html#section-1.3
const A=Buffer.allocUnsafe(0);
/** @type {PropertyDescriptor} */return Mr={uid:"258EAFA5-E914-47DA-95CA-C5AB0DC85B11",sentCloseFrameState:{NOT_SENT:0,PROCESSING:1,SENT:2},staticPropertyDescriptors:{enumerable:!0,writable:!1,configurable:!1},states:{CONNECTING:0,OPEN:1,CLOSING:2,CLOSED:3},opcodes:{CONTINUATION:0,TEXT:1,BINARY:2,CLOSE:8,PING:9,PONG:10},maxUnsigned16Bit:65535,parserStates:{INFO:0,PAYLOADLENGTH_16:2,PAYLOADLENGTH_64:3,READ_DATA:4},emptyBuffer:A,sendHints:{string:1,typedArray:2,arrayBuffer:3,blob:4}}}function Nn(){return Tr?Lr:(Tr=1,Lr={kWebSocketURL:Symbol("url"),kReadyState:Symbol("ready state"),kController:Symbol("controller"),kResponse:Symbol("response"),kBinaryType:Symbol("binary type"),kSentClose:Symbol("sent close"),kReceivedClose:Symbol("received close"),kByteParser:Symbol("byte parser")})}function Mn(){if(Yr)return Gr;Yr=1;const{kReadyState:A,kController:e,kResponse:t,kBinaryType:r,kWebSocketURL:n}=Nn(),{states:s,opcodes:o}=Sn(),{ErrorEvent:i,createFastMessageEvent:a}=Fn(),{isUtf8:g}=C,{collectASequenceOfCodePointsFast:c,removeHTTPWhitespace:Q}=Se();
/**
	 * @see https://dom.spec.whatwg.org/#concept-event-fire
	 * @param {string} e
	 * @param {EventTarget} target
	 * @param {(...args: ConstructorParameters<typeof Event>) => Event} eventFactory
	 * @param {EventInit | undefined} eventInitDict
	 */
function E(A,e,t=(A,e)=>new Event(A,e),r={}){
// 1. If eventConstructor is not given, then let eventConstructor be Event.
// 2. Let event be the result of creating an event given eventConstructor,
//    in the relevant realm of target.
// 3. Initialize event’s type attribute to e.
const n=t(A,r);
// 4. Initialize any other IDL attributes of event as described in the
//    invocation of this algorithm.
// 5. Return the result of dispatching event at target, with legacy target
//    override flag set if set.
e.dispatchEvent(n)}
/**
	 * @see https://websockets.spec.whatwg.org/#feedback-from-the-protocol
	 * @param {import('./websocket').WebSocket} ws
	 * @param {number} type Opcode
	 * @param {Buffer} data application data
	 */
/**
	 * @param {import('./websocket').WebSocket} ws
	 * @param {string|undefined} reason
	 */
function B(A,r){const{[e]:n,[t]:s}=A;n.abort(),s?.socket&&!s.socket.destroyed&&s.socket.destroy(),r&&
// TODO: process.nextTick
E("error",A,(A,e)=>new i(A,e),{error:new Error(r),message:r})}
/**
	 * @see https://datatracker.ietf.org/doc/html/rfc6455#section-5.5
	 * @param {number} opcode
	 */function h(A){return A===o.CLOSE||A===o.PING||A===o.PONG}function I(A){return A===o.CONTINUATION}function l(A){return A===o.TEXT||A===o.BINARY}
// https://nodejs.org/api/intl.html#detecting-internationalization-support
const u="string"==typeof process.versions.icu,d=u?new TextDecoder("utf-8",{fatal:!0}):void 0,f=u?d.decode.bind(d):function(A){if(g(A))return A.toString("utf-8");throw new TypeError("Invalid utf-8 received.")};return Gr={isConnecting:
/* globals Blob */
/**
	 * @param {import('./websocket').WebSocket} ws
	 * @returns {boolean}
	 */
function(e){
// If the WebSocket connection is not yet established, and the connection
// is not yet closed, then the WebSocket connection is in the CONNECTING state.
return e[A]===s.CONNECTING}
/**
	 * @param {import('./websocket').WebSocket} ws
	 * @returns {boolean}
	 */,isEstablished:function(e){
// If the server's response is validated as provided for above, it is
// said that _The WebSocket Connection is Established_ and that the
// WebSocket Connection is in the OPEN state.
return e[A]===s.OPEN}
/**
	 * @param {import('./websocket').WebSocket} ws
	 * @returns {boolean}
	 */,isClosing:function(e){
// Upon either sending or receiving a Close control frame, it is said
// that _The WebSocket Closing Handshake is Started_ and that the
// WebSocket connection is in the CLOSING state.
return e[A]===s.CLOSING}
/**
	 * @param {import('./websocket').WebSocket} ws
	 * @returns {boolean}
	 */,isClosed:function(e){return e[A]===s.CLOSED},fireEvent:E,isValidSubprotocol:
/**
	 * @see https://datatracker.ietf.org/doc/html/rfc6455
	 * @see https://datatracker.ietf.org/doc/html/rfc2616
	 * @see https://bugs.chromium.org/p/chromium/issues/detail?id=398407
	 * @param {string} protocol
	 */
function(A){
// If present, this value indicates one
// or more comma-separated subprotocol the client wishes to speak,
// ordered by preference.  The elements that comprise this value
// MUST be non-empty strings with characters in the range U+0021 to
// U+007E not including separator characters as defined in
// [RFC2616] and MUST all be unique strings.
if(0===A.length)return!1;for(let e=0;e<A.length;++e){const t=A.charCodeAt(e);if(t<33||// CTL, contains SP (0x20) and HT (0x09)
t>126||34===t||// "
40===t||// (
41===t||// )
44===t||// ,
47===t||// /
58===t||// :
59===t||// ;
60===t||// <
61===t||// =
62===t||// >
63===t||// ?
64===t||// @
91===t||// [
92===t||// \
93===t||// ]
123===t||// {
125===t)return!1}return!0}
/**
	 * @see https://datatracker.ietf.org/doc/html/rfc6455#section-7-4
	 * @param {number} code
	 */,isValidStatusCode:function(A){return A>=1e3&&A<1015?1004!==A&&// reserved
1005!==A&&// "MUST NOT be set as a status code"
1006!==A:A>=3e3&&A<=4999},failWebsocketConnection:B,websocketMessageReceived:function(e,t,i){
// 1. If ready state is not OPEN (1), then return.
if(e[A]!==s.OPEN)return;
// 2. Let dataForEvent be determined by switching on type and binary type:
let g;if(t===o.TEXT)
// -> type indicates that the data is Text
//      a new DOMString containing data
try{g=f(i)}catch{return void B(e,"Received invalid UTF-8 in text frame.")}else t===o.BINARY&&(
// -> type indicates that the data is Binary and binary type is "blob"
//      a new Blob object, created in the relevant Realm of the WebSocket
//      object, that represents data as its raw data
g="blob"===e[r]?new Blob([i]):function(A){if(A.byteLength===A.buffer.byteLength)return A.buffer;return A.buffer.slice(A.byteOffset,A.byteOffset+A.byteLength)}(i));
// 3. Fire an event named message at the WebSocket object, using MessageEvent,
//    with the origin attribute initialized to the serialization of the WebSocket
//    object’s url's origin, and the data attribute initialized to dataForEvent.
E("message",e,a,{origin:e[n].origin,data:g})},utf8Decode:f,isControlFrame:h,isContinuationFrame:I,isTextBinaryFrame:l,isValidOpcode:function(A){return l(A)||I(A)||h(A)}
/**
	 * Parses a Sec-WebSocket-Extensions header value.
	 * @param {string} extensions
	 * @returns {Map<string, string>}
	 */
// TODO(@Uzlopak, @KhafraDev): make compliant https://datatracker.ietf.org/doc/html/rfc6455#section-9.1
,parseExtensions:function(A){const e={position:0},t=new Map;for(;e.position<A.length;){const r=c(";",A,e),[n,s=""]=r.split("=");t.set(Q(n,!0,!1),Q(s,!1,!0)),e.position++}return t}
/**
	 * @see https://www.rfc-editor.org/rfc/rfc7692#section-7.1.2.2
	 * @description "client-max-window-bits = 1*DIGIT"
	 * @param {string} value
	 */,isValidClientWindowBits:function(A){
// Must have at least one character
if(0===A.length)return!1;
// Check all characters are ASCII digits
for(let e=0;e<A.length;e++){const t=A.charCodeAt(e);if(t<48||t>57)return!1}
// Check numeric range: zlib requires windowBits in range 8-15
const e=Number.parseInt(A,10);return e>=8&&e<=15}}}function Un(){if(Jr)return vr;Jr=1;const{maxUnsigned16Bit:A}=Sn(),e=16386;
/** @type {import('crypto')} */
let t,r=null,n=e;try{t=require("node:crypto");
/* c8 ignore next 3 */}catch{t={
// not full compatibility, but minimum.
randomFillSync:function(A,e,t){for(let e=0;e<A.length;++e)A[e]=255*Math.random()|0;return A}}}function s(){return n===e&&(n=0,t.randomFillSync(r??=Buffer.allocUnsafe(e),0,e)),[r[n++],r[n++],r[n++],r[n++]]}return vr={WebsocketFrameSend:class{
/**
	   * @param {Buffer|undefined} data
	   */
constructor(A){this.frameData=A}createFrame(e){const t=this.frameData,r=s(),n=t?.byteLength??0;
/** @type {number} */
let o=n,i=6;// 0-125
n>A?(i+=8,// payload length is next 8 bytes
o=127):n>125&&(i+=2,// payload length is next 2 bytes
o=126);const a=Buffer.allocUnsafe(n+i);
// Clear first 2 bytes, everything else is overwritten
a[0]=a[1]=0,a[0]|=128,// FIN
a[0]=(240&a[0])+e,// opcode
/*! ws. MIT License. Einar Otto Stangvik <einaros@gmail.com> */
a[i-4]=r[0],a[i-3]=r[1],a[i-2]=r[2],a[i-1]=r[3],a[1]=o,126===o?a.writeUInt16BE(n,2):127===o&&(
// Clear extended payload length
a[2]=a[3]=0,a.writeUIntBE(n,4,6)),a[1]|=128;// MASK
// mask body
for(let A=0;A<n;++A)a[i+A]=t[A]^r[3&A];return a}}},vr}function Ln(){if(Vr)return Hr;Vr=1;const{uid:A,states:e,sentCloseFrameState:t,emptyBuffer:r,opcodes:n}=Sn(),{kReadyState:s,kSentClose:o,kByteParser:i,kReceivedClose:a,kResponse:g}=Nn(),{fireEvent:c,failWebsocketConnection:Q,isClosing:E,isClosed:B,isEstablished:h,parseExtensions:I}=Mn(),{channels:C}=lA(),{CloseEvent:l}=Fn(),{makeRequest:u}=pn(),{fetching:d}=wn(),{Headers:f,getHeadersList:p}=un(),{getDecodeSplit:w}=Me(),{WebsocketFrameSend:y}=Un();
/** @type {import('crypto')} */
let D;try{D=require("node:crypto");
/* c8 ignore next 3 */}catch{}
/**
	 * @see https://websockets.spec.whatwg.org/#concept-websocket-establish
	 * @param {URL} url
	 * @param {string|string[]} protocols
	 * @param {import('./websocket').WebSocket} ws
	 * @param {(response: any, extensions: string[] | undefined) => void} onEstablish
	 * @param {Partial<import('../../types/websocket').WebSocketInit>} options
	 */
/**
	 * @param {Buffer} chunk
	 */
function m(A){this.ws[i].write(A)||this.pause()}
/**
	 * @see https://websockets.spec.whatwg.org/#feedback-from-the-protocol
	 * @see https://datatracker.ietf.org/doc/html/rfc6455#section-7.1.4
	 */function k(){const{ws:A}=this,{[g]:r}=A;r.socket.off("data",m),r.socket.off("close",k),r.socket.off("error",R);
// If the TCP connection was closed after the
// WebSocket closing handshake was completed, the WebSocket connection
// is said to have been closed _cleanly_.
const n=A[o]===t.SENT&&A[a];let Q=1005,E="";const B=A[i].closingInfo;B&&!B.error?(Q=B.code??1005,E=B.reason):A[a]||(
// If _The WebSocket
// Connection is Closed_ and no Close control frame was received by the
// endpoint (such as could occur if the underlying transport connection
// is lost), _The WebSocket Connection Close Code_ is considered to be
// 1006.
Q=1006),
// 1. Change the ready state to CLOSED (3).
A[s]=e.CLOSED,
// 2. If the user agent was required to fail the WebSocket
//    connection, or if the WebSocket connection was closed
//    after being flagged as full, fire an event named error
//    at the WebSocket object.
// TODO
// 3. Fire an event named close at the WebSocket object,
//    using CloseEvent, with the wasClean attribute
//    initialized to true if the connection closed cleanly
//    and false otherwise, the code attribute initialized to
//    the WebSocket connection close code, and the reason
//    attribute initialized to the result of applying UTF-8
//    decode without BOM to the WebSocket connection close
//    reason.
// TODO: process.nextTick
c("close",A,(A,e)=>new l(A,e),{wasClean:n,code:Q,reason:E}),C.close.hasSubscribers&&C.close.publish({websocket:A,code:Q,reason:E})}function R(A){const{ws:t}=this;t[s]=e.CLOSING,C.socketError.hasSubscribers&&C.socketError.publish(A),this.destroy()}return Hr={establishWebSocketConnection:function(e,t,r,n,s,o){
// 1. Let requestURL be a copy of url, with its scheme set to "http", if url’s
//    scheme is "ws", and to "https" otherwise.
const i=e;i.protocol="ws:"===e.protocol?"http:":"https:";
// 2. Let request be a new request, whose URL is requestURL, client is client,
//    service-workers mode is "none", referrer is "no-referrer", mode is
//    "websocket", credentials mode is "include", cache mode is "no-store" ,
//    and redirect mode is "error".
const a=u({urlList:[i],client:r,serviceWorkers:"none",referrer:"no-referrer",mode:"websocket",credentials:"include",cache:"no-store",redirect:"error"});
// Note: undici extension, allow setting custom headers.
if(o.headers){const A=p(new f(o.headers));a.headersList=A}
// 3. Append (`Upgrade`, `websocket`) to request’s header list.
// 4. Append (`Connection`, `Upgrade`) to request’s header list.
// Note: both of these are handled by undici currently.
// https://github.com/nodejs/undici/blob/68c269c4144c446f3f1220951338daef4a6b5ec4/lib/client.js#L1397
// 5. Let keyValue be a nonce consisting of a randomly selected
//    16-byte value that has been forgiving-base64-encoded and
//    isomorphic encoded.
const g=D.randomBytes(16).toString("base64");
// 6. Append (`Sec-WebSocket-Key`, keyValue) to request’s
//    header list.
a.headersList.append("sec-websocket-key",g),
// 7. Append (`Sec-WebSocket-Version`, `13`) to request’s
//    header list.
a.headersList.append("sec-websocket-version","13");
// 8. For each protocol in protocols, combine
//    (`Sec-WebSocket-Protocol`, protocol) in request’s header
//    list.
for(const A of t)a.headersList.append("sec-websocket-protocol",A);
// 9. Let permessageDeflate be a user-agent defined
//    "permessage-deflate" extension header value.
// https://github.com/mozilla/gecko-dev/blob/ce78234f5e653a5d3916813ff990f053510227bc/netwerk/protocol/websocket/WebSocketChannel.cpp#L2673
// 10. Append (`Sec-WebSocket-Extensions`, permessageDeflate) to
//     request’s header list.
a.headersList.append("sec-websocket-extensions","permessage-deflate; client_max_window_bits");
// 11. Fetch request with useParallelQueue set to true, and
//     processResponse given response being these steps:
const c=d({request:a,useParallelQueue:!0,dispatcher:o.dispatcher,processResponse(e){
// 1. If response is a network error or its status is not 101,
//    fail the WebSocket connection.
if("error"===e.type||101!==e.status)return void Q(n,"Received network error or non-101 status code.");
// 2. If protocols is not the empty list and extracting header
//    list values given `Sec-WebSocket-Protocol` and response’s
//    header list results in null, failure, or the empty byte
//    sequence, then fail the WebSocket connection.
if(0!==t.length&&!e.headersList.get("Sec-WebSocket-Protocol"))return void Q(n,"Server did not respond with sent protocols.");
// 3. Follow the requirements stated step 2 to step 6, inclusive,
//    of the last set of steps in section 4.1 of The WebSocket
//    Protocol to validate response. This either results in fail
//    the WebSocket connection or the WebSocket connection is
//    established.
// 2. If the response lacks an |Upgrade| header field or the |Upgrade|
//    header field contains a value that is not an ASCII case-
//    insensitive match for the value "websocket", the client MUST
//    _Fail the WebSocket Connection_.
if("websocket"!==e.headersList.get("Upgrade")?.toLowerCase())return void Q(n,'Server did not set Upgrade header to "websocket".');
// 3. If the response lacks a |Connection| header field or the
//    |Connection| header field doesn't contain a token that is an
//    ASCII case-insensitive match for the value "Upgrade", the client
//    MUST _Fail the WebSocket Connection_.
if("upgrade"!==e.headersList.get("Connection")?.toLowerCase())return void Q(n,'Server did not set Connection header to "upgrade".');
// 4. If the response lacks a |Sec-WebSocket-Accept| header field or
//    the |Sec-WebSocket-Accept| contains a value other than the
//    base64-encoded SHA-1 of the concatenation of the |Sec-WebSocket-
//    Key| (as a string, not base64-decoded) with the string "258EAFA5-
//    E914-47DA-95CA-C5AB0DC85B11" but ignoring any leading and
//    trailing whitespace, the client MUST _Fail the WebSocket
//    Connection_.
if(e.headersList.get("Sec-WebSocket-Accept")!==D.createHash("sha1").update(g+A).digest("base64"))return void Q(n,"Incorrect hash received in Sec-WebSocket-Accept header.");
// 5. If the response includes a |Sec-WebSocket-Extensions| header
//    field and this header field indicates the use of an extension
//    that was not present in the client's handshake (the server has
//    indicated an extension not requested by the client), the client
//    MUST _Fail the WebSocket Connection_.  (The parsing of this
//    header field to determine which extensions are requested is
//    discussed in Section 9.1.)
const r=e.headersList.get("Sec-WebSocket-Extensions");let o;if(null!==r&&(o=I(r),!o.has("permessage-deflate")))return void Q(n,"Sec-WebSocket-Extensions header does not match.");
// 6. If the response includes a |Sec-WebSocket-Protocol| header field
//    and this header field indicates the use of a subprotocol that was
//    not present in the client's handshake (the server has indicated a
//    subprotocol not requested by the client), the client MUST _Fail
//    the WebSocket Connection_.
const i=e.headersList.get("Sec-WebSocket-Protocol");if(null!==i){const A=w("sec-websocket-protocol",a.headersList);
// The client can request that the server use a specific subprotocol by
// including the |Sec-WebSocket-Protocol| field in its handshake.  If it
// is specified, the server needs to include the same field and one of
// the selected subprotocol values in its response for the connection to
// be established.
if(null===A||!A.includes(i))return void Q(n,"Protocol was not set in the opening handshake.")}e.socket.on("data",m),e.socket.on("close",k),e.socket.on("error",R),C.open.hasSubscribers&&C.open.publish({address:e.socket.address(),protocol:i,extensions:r}),s(e,o)}});return c},closeWebSocketConnection:function(A,i,a,c){if(E(A)||B(A));else if(h(A))if(A[o]===t.NOT_SENT){
// If the WebSocket closing handshake has not yet been started
// Start the WebSocket closing handshake and set this's ready
// state to CLOSING (2).
// - If neither code nor reason is present, the WebSocket Close
//   message must not have a body.
// - If code is present, then the status code to use in the
//   WebSocket Close message must be the integer given by code.
// - If reason is also present, then reasonBytes must be
//   provided in the Close message after the status code.
A[o]=t.PROCESSING;const Q=new y;
// If neither code nor reason is present, the WebSocket Close
// message must not have a body.
// If code is present, then the status code to use in the
// WebSocket Close message must be the integer given by code.
void 0!==i&&void 0===a?(Q.frameData=Buffer.allocUnsafe(2),Q.frameData.writeUInt16BE(i,0)):void 0!==i&&void 0!==a?(
// If reason is also present, then reasonBytes must be
// provided in the Close message after the status code.
Q.frameData=Buffer.allocUnsafe(2+c),Q.frameData.writeUInt16BE(i,0),
// the body MAY contain UTF-8-encoded data with value /reason/
Q.frameData.write(a,2,"utf-8")):Q.frameData=r
/** @type {import('stream').Duplex} */;A[g].socket.write(Q.createFrame(n.CLOSE)),A[o]=t.SENT,
// Upon either sending or receiving a Close control frame, it is said
// that _The WebSocket Closing Handshake is Started_ and that the
// WebSocket connection is in the CLOSING state.
A[s]=e.CLOSING}else
// Otherwise
// Set this's ready state to CLOSING (2).
A[s]=e.CLOSING;else
// If the WebSocket connection is not yet established
// Fail the WebSocket connection and set this's ready state
// to CLOSING (2).
Q(A,"Connection was closed before it was established."),A[s]=e.CLOSING}},Hr}function Tn(){if(Pr)return Or;Pr=1;const{Writable:A}=I,e=E,{parserStates:t,opcodes:r,states:n,emptyBuffer:s,sentCloseFrameState:o}=Sn(),{kReadyState:i,kSentClose:a,kResponse:g,kReceivedClose:c}=Nn(),{channels:Q}=lA(),{isValidStatusCode:B,isValidOpcode:h,failWebsocketConnection:C,websocketMessageReceived:l,utf8Decode:u,isControlFrame:d,isTextBinaryFrame:f,isContinuationFrame:p}=Mn(),{WebsocketFrameSend:y}=Un(),{closeWebSocketConnection:D}=Ln(),{PerMessageDeflate:m}=function(){if(Wr)return xr;Wr=1;const{createInflateRaw:A,Z_DEFAULT_WINDOWBITS:e}=w,{isValidClientWindowBits:t}=Mn(),{MessageSizeExceededError:r}=hA(),n=Buffer.from([0,0,255,255]),s=Symbol("kBuffer"),o=Symbol("kLength");return xr={PerMessageDeflate:class{
/** @type {import('node:zlib').InflateRaw} */
#L;#T={};#G=0
/**
	   * @param {Map<string, string>} extensions
	   */;constructor(A,e){this.#T.serverNoContextTakeover=A.has("server_no_context_takeover"),this.#T.serverMaxWindowBits=A.get("server_max_window_bits"),this.#G=e.maxPayloadSize}
/**
	   * Decompress a compressed payload.
	   * @param {Buffer} chunk Compressed data
	   * @param {boolean} fin Final fragment flag
	   * @param {Function} callback Callback function
	   */decompress(i,a,g){
// An endpoint uses the following algorithm to decompress a message.
// 1.  Append 4 octets of 0x00 0x00 0xff 0xff to the tail end of the
//     payload of the message.
// 2.  Decompress the resulting data using DEFLATE.
if(!this.#L){let n=e;if(this.#T.serverMaxWindowBits){// empty values default to Z_DEFAULT_WINDOWBITS
if(!t(this.#T.serverMaxWindowBits))return void g(new Error("Invalid server_max_window_bits"));n=Number.parseInt(this.#T.serverMaxWindowBits)}try{this.#L=A({windowBits:n})}catch(A){return void g(A)}this.#L[s]=[],this.#L[o]=0,this.#L.on("data",A=>{if(this.#L[o]+=A.length,this.#G>0&&this.#L[o]>this.#G)return g(new r),
// The inflater may still hold buffered input that can emit a late
// zlib error. Remove the data listener, then deterministically stop
// the stream so a subsequent 'error' cannot fire without a listener
// (which would terminate the process as an unhandled error event).
this.#L.removeAllListeners(),this.#L.destroy(),void(this.#L=null);this.#L[s].push(A)}),this.#L.on("error",A=>{this.#L=null,g(A)})}this.#L.write(i),a&&this.#L.write(n),this.#L.flush(()=>{if(!this.#L)return;const A=Buffer.concat(this.#L[s],this.#L[o]);this.#L[s].length=0,this.#L[o]=0,g(null,A)})}}}}(),{MessageSizeExceededError:k}=hA();function R(A,e,t){D(A,e,t,Buffer.byteLength(t)),C(A,t)}
// This code was influenced by ws released under the MIT license.
// Copyright (c) 2011 Einar Otto Stangvik <einaros@gmail.com>
// Copyright (c) 2013 Arnout Kazemier and contributors
// Copyright (c) 2016 Luigi Pinca and contributors
return Or={ByteParser:class extends A{#Y=[];#v=0;#J=0;#H=!1;#y=t.INFO;#V={};#x=[]
/** @type {Map<string, PerMessageDeflate>} */;#W;
/** @type {number} */#O;
/** @type {number} */#G;
/**
	   * @param {import('./websocket').WebSocket} ws
	   * @param {Map<string, string>|null} extensions
	   * @param {{ maxFragments?: number, maxPayloadSize?: number }} [options]
	   */constructor(A,e,t={}){super(),this.ws=A,this.#W=e??new Map,this.#O=t.maxFragments??0,this.#G=t.maxPayloadSize??0,this.#W.has("permessage-deflate")&&this.#W.set("permessage-deflate",new m(e,t))}
/**
	   * @param {Buffer} chunk
	   * @param {() => void} callback
	   */_write(A,e,t){this.#Y.push(A),this.#J+=A.length,this.#H=!0,this.run(t)}#P(){return!(this.#G>0&&!d(this.#V.opcode)&&this.#V.payloadLength+this.#v>this.#G)||(R(this.ws,1009,"Payload size exceeds maximum allowed size"),!1)}
/**
	   * Runs whenever a new chunk is received.
	   * Callback is called whenever there are no more chunks buffering,
	   * or not enough bytes are buffered to parse.
	   */run(A){for(;this.#H;)if(this.#y===t.INFO){
// If there aren't enough bytes to parse the payload length, etc.
if(this.#J<2)return A();const e=this.consume(2),n=!!(128&e[0]),s=15&e[0],o=!(128&~e[1]),i=!n&&s!==r.CONTINUATION,a=127&e[1],g=64&e[0],c=32&e[0],Q=16&e[0];if(!h(s))return C(this.ws,"Invalid opcode received"),A();if(o)return C(this.ws,"Frame cannot be masked"),A();
// MUST be 0 unless an extension is negotiated that defines meanings
// for non-zero values.  If a nonzero value is received and none of
// the negotiated extensions defines the meaning of such a nonzero
// value, the receiving endpoint MUST _Fail the WebSocket
// Connection_.
// This document allocates the RSV1 bit of the WebSocket header for
// PMCEs and calls the bit the "Per-Message Compressed" bit.  On a
// WebSocket connection where a PMCE is in use, this bit indicates
// whether a message is compressed or not.
if(0!==g&&!this.#W.has("permessage-deflate"))return void C(this.ws,"Expected RSV1 to be clear.");if(0!==c||0!==Q)return void C(this.ws,"RSV1, RSV2, RSV3 must be clear");if(i&&!f(s))
// Only text and binary frames can be fragmented
return void C(this.ws,"Invalid frame type was fragmented.");
// If we are already parsing a text/binary frame and do not receive either
// a continuation frame or close frame, fail the connection.
if(f(s)&&this.#x.length>0)return void C(this.ws,"Expected continuation frame");if(this.#V.fragmented&&i)
// A fragmented frame can't be fragmented itself
return void C(this.ws,"Fragmented frame exceeded 125 bytes.");
// "All control frames MUST have a payload length of 125 bytes or less
// and MUST NOT be fragmented."
if((a>125||i)&&d(s))return void C(this.ws,"Control frame either too large or fragmented");if(p(s)&&0===this.#x.length&&!this.#V.compressed)return void C(this.ws,"Unexpected continuation frame");if(a<=125){if(this.#V.payloadLength=a,this.#y=t.READ_DATA,!this.#P())return}else 126===a?this.#y=t.PAYLOADLENGTH_16:127===a&&(this.#y=t.PAYLOADLENGTH_64);f(s)&&(this.#V.binaryType=s,this.#V.compressed=0!==g),this.#V.opcode=s,this.#V.masked=o,this.#V.fin=n,this.#V.fragmented=i}else if(this.#y===t.PAYLOADLENGTH_16){if(this.#J<2)return A();const e=this.consume(2);if(this.#V.payloadLength=e.readUInt16BE(0),this.#y=t.READ_DATA,!this.#P())return}else if(this.#y===t.PAYLOADLENGTH_64){if(this.#J<8)return A();const e=this.consume(8),r=e.readUInt32BE(0),n=e.readUInt32BE(4);
// 2^31 is the maximum bytes an arraybuffer can contain
// on 32-bit systems. Although, on 64-bit systems, this is
// 2^53-1 bytes.
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Errors/Invalid_array_length
// https://source.chromium.org/chromium/chromium/src/+/main:v8/src/common/globals.h;drc=1946212ac0100668f14eb9e2843bdd846e510a1e;bpv=1;bpt=1;l=1275
// https://source.chromium.org/chromium/chromium/src/+/main:v8/src/objects/js-array-buffer.h;l=34;drc=1946212ac0100668f14eb9e2843bdd846e510a1e
if(0!==r||n>2**31-1)return void C(this.ws,"Received payload length > 2^31 bytes.");if(this.#V.payloadLength=n,this.#y=t.READ_DATA,!this.#P())return}else if(this.#y===t.READ_DATA){if(this.#J<this.#V.payloadLength)return A();const e=this.consume(this.#V.payloadLength);if(d(this.#V.opcode))this.#H=this.parseControlFrame(e),this.#y=t.INFO;else{if(this.#V.compressed){this.#W.get("permessage-deflate").decompress(e,this.#V.fin,(e,r)=>{if(e){const A=e instanceof k?1009:1007;return void R(this.ws,A,e.message)}if(this.writeFragments(r))if(this.#G>0&&this.#v>this.#G)R(this.ws,1009,(new k).message);else{if(!this.#V.fin)return this.#y=t.INFO,this.#H=!0,void this.run(A);l(this.ws,this.#V.binaryType,this.consumeFragments()),this.#H=!0,this.#y=t.INFO,this.run(A)}}),this.#H=!1;break}if(!this.writeFragments(e))return;if(this.#G>0&&this.#v>this.#G)return void R(this.ws,1009,(new k).message);
// If the frame is not fragmented, a message has been received.
// If the frame is fragmented, it will terminate with a fin bit set
// and an opcode of 0 (continuation), therefore we handle that when
// parsing continuation frames, not here.
!this.#V.fragmented&&this.#V.fin&&l(this.ws,this.#V.binaryType,this.consumeFragments()),this.#y=t.INFO}}}
/**
	   * Take n bytes from the buffered Buffers
	   * @param {number} n
	   * @returns {Buffer}
	   */consume(A){if(A>this.#J)throw new Error("Called consume() before buffers satiated.");if(0===A)return s;if(this.#Y[0].length===A)return this.#J-=this.#Y[0].length,this.#Y.shift();const e=Buffer.allocUnsafe(A);let t=0;for(;t!==A;){const r=this.#Y[0],{length:n}=r;if(n+t===A){e.set(this.#Y.shift(),t);break}if(n+t>A){e.set(r.subarray(0,A-t),t),this.#Y[0]=r.subarray(A-t);break}e.set(this.#Y.shift(),t),t+=r.length}return this.#J-=A,e}writeFragments(A){return this.#O>0&&this.#x.length===this.#O?(R(this.ws,1008,"Too many message fragments"),!1):(this.#v+=A.length,this.#x.push(A),!0)}consumeFragments(){const A=this.#x;if(1===A.length)return this.#v=0,A.shift();const e=Buffer.concat(A,this.#v);return this.#x=[],this.#v=0,e}parseCloseBody(A){
// https://datatracker.ietf.org/doc/html/rfc6455#section-7.1.5
/** @type {number|undefined} */
let t;if(e(1!==A.length),A.length>=2&&(
// _The WebSocket Connection Close Code_ is
// defined as the status code (Section 7.4) contained in the first Close
// control frame received by the application
t=A.readUInt16BE(0)),void 0!==t&&!B(t))return{code:1002,reason:"Invalid status code",error:!0};
// https://datatracker.ietf.org/doc/html/rfc6455#section-7.1.6
/** @type {Buffer} */let r=A.subarray(2);
// Remove BOM
239===r[0]&&187===r[1]&&191===r[2]&&(r=r.subarray(3));try{r=u(r)}catch{return{code:1007,reason:"Invalid UTF-8",error:!0}}return{code:t,reason:r,error:!1}}
/**
	   * Parses control frames.
	   * @param {Buffer} body
	   */parseControlFrame(A){const{opcode:e,payloadLength:t}=this.#V;if(e===r.CLOSE){if(1===t)return C(this.ws,"Received close frame with a 1-byte body."),!1;if(this.#V.closeInfo=this.parseCloseBody(A),this.#V.closeInfo.error){const{code:A,reason:e}=this.#V.closeInfo;return D(this.ws,A,e,e.length),C(this.ws,e),!1}if(this.ws[a]!==o.SENT){
// If an endpoint receives a Close frame and did not previously send a
// Close frame, the endpoint MUST send a Close frame in response.  (When
// sending a Close frame in response, the endpoint typically echos the
// status code it received.)
let A=s;this.#V.closeInfo.code&&(A=Buffer.allocUnsafe(2),A.writeUInt16BE(this.#V.closeInfo.code,0));const e=new y(A);this.ws[g].socket.write(e.createFrame(r.CLOSE),A=>{A||(this.ws[a]=o.SENT)})}
// Upon either sending or receiving a Close control frame, it is said
// that _The WebSocket Closing Handshake is Started_ and that the
// WebSocket connection is in the CLOSING state.
return this.ws[i]=n.CLOSING,this.ws[c]=!0,!1}if(e===r.PING){
// Upon receipt of a Ping frame, an endpoint MUST send a Pong frame in
// response, unless it already received a Close frame.
// A Pong frame sent in response to a Ping frame must have identical
// "Application data"
if(!this.ws[c]){const e=new y(A);this.ws[g].socket.write(e.createFrame(r.PONG)),Q.ping.hasSubscribers&&Q.ping.publish({payload:A})}}else e===r.PONG&&Q.pong.hasSubscribers&&Q.pong.publish({payload:A});return!0}get closingInfo(){return this.#V.closeInfo}}},Or}function Gn(){if(Zr)return qr;Zr=1;const{WebsocketFrameSend:A}=Un(),{opcodes:e,sendHints:t}=Sn(),r=xe(),n=Buffer[Symbol.species];function s(r,s){return new A(function(A,e){switch(e){case t.string:return Buffer.from(A);case t.arrayBuffer:case t.blob:return new n(A);case t.typedArray:return new n(A.buffer,A.byteOffset,A.byteLength)}}(r,s)).createFrame(s===t.string?e.TEXT:e.BINARY)}return qr={SendQueue:
/**
	 * @typedef {object} SendQueueNode
	 * @property {Promise<void> | null} promise
	 * @property {((...args: any[]) => any)} callback
	 * @property {Buffer | null} frame
	 */
class{
/**
	   * @type {FixedQueue}
	   */
#q=new r
/**
	   * @type {boolean}
	   */;#Z=!1
/** @type {import('node:net').Socket} */;#z;constructor(A){this.#z=A}add(A,e,r){if(r!==t.blob){const t=s(A,r);if(this.#Z){
/** @type {SendQueueNode} */
const A={promise:null,callback:e,frame:t};this.#q.push(A)}else
// fast-path
this.#z.write(t,e);return}
/** @type {SendQueueNode} */const n={promise:A.arrayBuffer().then(A=>{n.promise=null,n.frame=s(A,r)}),callback:e,frame:null};this.#q.push(n),this.#Z||this.#K()}async#K(){this.#Z=!0;const A=this.#q;for(;!A.isEmpty();){const e=A.shift();
// wait pending promise
null!==e.promise&&await e.promise,
// write
this.#z.write(e.frame,e.callback),
// cleanup
e.callback=e.frame=null}this.#Z=!1}}},qr}function Yn(){if(Kr)return zr;Kr=1;const{webidl:A}=Ne(),{URLSerializer:e}=Se(),{environmentSettingsObject:t}=Me(),{staticPropertyDescriptors:r,states:n,sentCloseFrameState:s,sendHints:o}=Sn(),{kWebSocketURL:i,kReadyState:a,kController:g,kBinaryType:c,kResponse:Q,kSentClose:E,kByteParser:B}=Nn(),{isConnecting:h,isEstablished:I,isClosing:C,isValidSubprotocol:u,fireEvent:d}=Mn(),{establishWebSocketConnection:f,closeWebSocketConnection:p}=Ln(),{ByteParser:w}=Tn(),{kEnumerableProperty:y,isBlobLike:D}=CA(),{getGlobalDispatcher:m}=En(),{types:k}=l,{ErrorEvent:R,CloseEvent:b}=Fn(),{SendQueue:F}=Gn();
// https://websockets.spec.whatwg.org/#interface-definition
class S extends EventTarget{#X={open:null,error:null,close:null,message:null};#_=0;#j="";#W=""
/** @type {SendQueue} */;#$;
/**
	   * @param {string} url
	   * @param {string|string[]} protocols
	   */constructor(e,r=[]){super(),A.util.markAsUncloneable(this);const n="WebSocket constructor";A.argumentLengthCheck(arguments,1,n);const o=A.converters["DOMString or sequence<DOMString> or WebSocketInit"](r,n,"options");e=A.converters.USVString(e,n,"url"),r=o.protocols;
// 1. Let baseURL be this's relevant settings object's API base URL.
const Q=t.settingsObject.baseUrl;
// 1. Let urlRecord be the result of applying the URL parser to url with baseURL.
let B;try{B=new URL(e,Q)}catch(A){
// 3. If urlRecord is failure, then throw a "SyntaxError" DOMException.
throw new DOMException(A,"SyntaxError")}
// 4. If urlRecord’s scheme is "http", then set urlRecord’s scheme to "ws".
// 6. If urlRecord’s scheme is not "ws" or "wss", then throw a "SyntaxError" DOMException.
if("http:"===B.protocol?B.protocol="ws:":"https:"===B.protocol&&(
// 5. Otherwise, if urlRecord’s scheme is "https", set urlRecord’s scheme to "wss".
B.protocol="wss:"),"ws:"!==B.protocol&&"wss:"!==B.protocol)throw new DOMException(`Expected a ws: or wss: protocol, got ${B.protocol}`,"SyntaxError");
// 7. If urlRecord’s fragment is non-null, then throw a "SyntaxError"
//    DOMException.
if(B.hash||B.href.endsWith("#"))throw new DOMException("Got fragment","SyntaxError");
// 8. If protocols is a string, set protocols to a sequence consisting
//    of just that string.
// 9. If any of the values in protocols occur more than once or otherwise
//    fail to match the requirements for elements that comprise the value
//    of `Sec-WebSocket-Protocol` fields as defined by The WebSocket
//    protocol, then throw a "SyntaxError" DOMException.
if("string"==typeof r&&(r=[r]),r.length!==new Set(r.map(A=>A.toLowerCase())).size)throw new DOMException("Invalid Sec-WebSocket-Protocol value","SyntaxError");if(r.length>0&&!r.every(A=>u(A)))throw new DOMException("Invalid Sec-WebSocket-Protocol value","SyntaxError");
// 10. Set this's url to urlRecord.
this[i]=new URL(B.href);
// 11. Let client be this's relevant settings object.
const h=t.settingsObject;
// 12. Run this step in parallel:
//    1. Establish a WebSocket connection given urlRecord, protocols,
//       and client.
this[g]=f(B,r,h,this,(A,e)=>this.#AA(A,e),o),
// Each WebSocket object has an associated ready state, which is a
// number representing the state of the connection. Initially it must
// be CONNECTING (0).
this[a]=S.CONNECTING,this[E]=s.NOT_SENT,
// The extensions attribute must initially return the empty string.
// The protocol attribute must initially return the empty string.
// Each WebSocket object has an associated binary type, which is a
// BinaryType. Initially it must be "blob".
this[c]="blob"}
/**
	   * @see https://websockets.spec.whatwg.org/#dom-websocket-close
	   * @param {number|undefined} code
	   * @param {string|undefined} reason
	   */close(e=void 0,t=void 0){A.brandCheck(this,S);const r="WebSocket.close";
// 1. If code is present, but is neither an integer equal to 1000 nor an
//    integer in the range 3000 to 4999, inclusive, throw an
//    "InvalidAccessError" DOMException.
if(void 0!==e&&(e=A.converters["unsigned short"](e,r,"code",{clamp:!0})),void 0!==t&&(t=A.converters.USVString(t,r,"reason")),void 0!==e&&1e3!==e&&(e<3e3||e>4999))throw new DOMException("invalid code","InvalidAccessError");let n=0;
// 2. If reason is present, then run these substeps:
if(void 0!==t&&(
// 1. Let reasonBytes be the result of encoding reason.
// 2. If reasonBytes is longer than 123 bytes, then throw a
//    "SyntaxError" DOMException.
n=Buffer.byteLength(t),n>123))throw new DOMException(`Reason must be less than 123 bytes; received ${n}`,"SyntaxError");
// 3. Run the first matching steps from the following list:
p(this,e,t,n)}
/**
	   * @see https://websockets.spec.whatwg.org/#dom-websocket-send
	   * @param {NodeJS.TypedArray|ArrayBuffer|Blob|string} data
	   */send(e){A.brandCheck(this,S);const t="WebSocket.send";
// 1. If this's ready state is CONNECTING, then throw an
//    "InvalidStateError" DOMException.
if(A.argumentLengthCheck(arguments,1,t),e=A.converters.WebSocketSendData(e,t,"data"),h(this))throw new DOMException("Sent before connected.","InvalidStateError");
// 2. Run the appropriate set of steps from the following list:
// https://datatracker.ietf.org/doc/html/rfc6455#section-6.1
// https://datatracker.ietf.org/doc/html/rfc6455#section-5.2
if(I(this)&&!C(this))
// If data is a string
if("string"==typeof e){
// If the WebSocket connection is established and the WebSocket
// closing handshake has not yet started, then the user agent
// must send a WebSocket Message comprised of the data argument
// using a text frame opcode; if the data cannot be sent, e.g.
// because it would need to be buffered but the buffer is full,
// the user agent must flag the WebSocket as full and then close
// the WebSocket connection. Any invocation of this method with a
// string argument that does not throw an exception must increase
// the bufferedAmount attribute by the number of bytes needed to
// express the argument as UTF-8.
const A=Buffer.byteLength(e);this.#_+=A,this.#$.add(e,()=>{this.#_-=A},o.string)}else k.isArrayBuffer(e)?(
// If the WebSocket connection is established, and the WebSocket
// closing handshake has not yet started, then the user agent must
// send a WebSocket Message comprised of data using a binary frame
// opcode; if the data cannot be sent, e.g. because it would need
// to be buffered but the buffer is full, the user agent must flag
// the WebSocket as full and then close the WebSocket connection.
// The data to be sent is the data stored in the buffer described
// by the ArrayBuffer object. Any invocation of this method with an
// ArrayBuffer argument that does not throw an exception must
// increase the bufferedAmount attribute by the length of the
// ArrayBuffer in bytes.
this.#_+=e.byteLength,this.#$.add(e,()=>{this.#_-=e.byteLength},o.arrayBuffer)):ArrayBuffer.isView(e)?(
// If the WebSocket connection is established, and the WebSocket
// closing handshake has not yet started, then the user agent must
// send a WebSocket Message comprised of data using a binary frame
// opcode; if the data cannot be sent, e.g. because it would need to
// be buffered but the buffer is full, the user agent must flag the
// WebSocket as full and then close the WebSocket connection. The
// data to be sent is the data stored in the section of the buffer
// described by the ArrayBuffer object that data references. Any
// invocation of this method with this kind of argument that does
// not throw an exception must increase the bufferedAmount attribute
// by the length of data’s buffer in bytes.
this.#_+=e.byteLength,this.#$.add(e,()=>{this.#_-=e.byteLength},o.typedArray)):D(e)&&(
// If the WebSocket connection is established, and the WebSocket
// closing handshake has not yet started, then the user agent must
// send a WebSocket Message comprised of data using a binary frame
// opcode; if the data cannot be sent, e.g. because it would need to
// be buffered but the buffer is full, the user agent must flag the
// WebSocket as full and then close the WebSocket connection. The data
// to be sent is the raw data represented by the Blob object. Any
// invocation of this method with a Blob argument that does not throw
// an exception must increase the bufferedAmount attribute by the size
// of the Blob object’s raw data, in bytes.
this.#_+=e.size,this.#$.add(e,()=>{this.#_-=e.size},o.blob))}get readyState(){
// The readyState getter steps are to return this's ready state.
return A.brandCheck(this,S),this[a]}get bufferedAmount(){return A.brandCheck(this,S),this.#_}get url(){
// The url getter steps are to return this's url, serialized.
return A.brandCheck(this,S),e(this[i])}get extensions(){return A.brandCheck(this,S),this.#W}get protocol(){return A.brandCheck(this,S),this.#j}get onopen(){return A.brandCheck(this,S),this.#X.open}set onopen(e){A.brandCheck(this,S),this.#X.open&&this.removeEventListener("open",this.#X.open),"function"==typeof e?(this.#X.open=e,this.addEventListener("open",e)):this.#X.open=null}get onerror(){return A.brandCheck(this,S),this.#X.error}set onerror(e){A.brandCheck(this,S),this.#X.error&&this.removeEventListener("error",this.#X.error),"function"==typeof e?(this.#X.error=e,this.addEventListener("error",e)):this.#X.error=null}get onclose(){return A.brandCheck(this,S),this.#X.close}set onclose(e){A.brandCheck(this,S),this.#X.close&&this.removeEventListener("close",this.#X.close),"function"==typeof e?(this.#X.close=e,this.addEventListener("close",e)):this.#X.close=null}get onmessage(){return A.brandCheck(this,S),this.#X.message}set onmessage(e){A.brandCheck(this,S),this.#X.message&&this.removeEventListener("message",this.#X.message),"function"==typeof e?(this.#X.message=e,this.addEventListener("message",e)):this.#X.message=null}get binaryType(){return A.brandCheck(this,S),this[c]}set binaryType(e){A.brandCheck(this,S),this[c]="blob"!==e&&"arraybuffer"!==e?"blob":e}
/**
	   * @see https://websockets.spec.whatwg.org/#feedback-from-the-protocol
	   */#AA(A,e){
// processResponse is called when the "response's header list has been received and initialized."
// once this happens, the connection is open
this[Q]=A;const t=this[g]?.dispatcher?.webSocketOptions,r=t?.maxFragments,s=t?.maxPayloadSize,o=new w(this,e,{maxFragments:r,maxPayloadSize:s});o.on("drain",N),o.on("error",M.bind(this)),A.socket.ws=this,this[B]=o,this.#$=new F(A.socket),
// 1. Change the ready state to OPEN (1).
this[a]=n.OPEN;
// 2. Change the extensions attribute’s value to the extensions in use, if
//    it is not the null value.
// https://datatracker.ietf.org/doc/html/rfc6455#section-9.1
const i=A.headersList.get("sec-websocket-extensions");null!==i&&(this.#W=i);
// 3. Change the protocol attribute’s value to the subprotocol in use, if
//    it is not the null value.
// https://datatracker.ietf.org/doc/html/rfc6455#section-1.9
const c=A.headersList.get("sec-websocket-protocol");null!==c&&(this.#j=c),
// 4. Fire an event named open at the WebSocket object.
d("open",this)}}
// https://websockets.spec.whatwg.org/#dom-websocket-connecting
function N(){this.ws[Q].socket.resume()}function M(A){let e,t;A instanceof b?(e=A.reason,t=A.code):e=A.message,d("error",this,()=>new R("error",{error:A,message:e})),p(this,t)}return S.CONNECTING=S.prototype.CONNECTING=n.CONNECTING,
// https://websockets.spec.whatwg.org/#dom-websocket-open
S.OPEN=S.prototype.OPEN=n.OPEN,
// https://websockets.spec.whatwg.org/#dom-websocket-closing
S.CLOSING=S.prototype.CLOSING=n.CLOSING,
// https://websockets.spec.whatwg.org/#dom-websocket-closed
S.CLOSED=S.prototype.CLOSED=n.CLOSED,Object.defineProperties(S.prototype,{CONNECTING:r,OPEN:r,CLOSING:r,CLOSED:r,url:y,readyState:y,bufferedAmount:y,onopen:y,onerror:y,onclose:y,close:y,onmessage:y,binaryType:y,send:y,extensions:y,protocol:y,[Symbol.toStringTag]:{value:"WebSocket",writable:!1,enumerable:!1,configurable:!0}}),Object.defineProperties(S,{CONNECTING:r,OPEN:r,CLOSING:r,CLOSED:r}),A.converters["sequence<DOMString>"]=A.sequenceConverter(A.converters.DOMString),A.converters["DOMString or sequence<DOMString>"]=function(e,t,r){return"Object"===A.util.Type(e)&&Symbol.iterator in e?A.converters["sequence<DOMString>"](e):A.converters.DOMString(e,t,r)},
// This implements the proposal made in https://github.com/whatwg/websockets/issues/42
A.converters.WebSocketInit=A.dictionaryConverter([{key:"protocols",converter:A.converters["DOMString or sequence<DOMString>"],defaultValue:()=>new Array(0)},{key:"dispatcher",converter:A.converters.any,defaultValue:()=>m()},{key:"headers",converter:A.nullableConverter(A.converters.HeadersInit)}]),A.converters["DOMString or sequence<DOMString> or WebSocketInit"]=function(e){return"Object"!==A.util.Type(e)||Symbol.iterator in e?{protocols:A.converters["DOMString or sequence<DOMString>"](e)}:A.converters.WebSocketInit(e)},A.converters.WebSocketSendData=function(e){if("Object"===A.util.Type(e)){if(D(e))return A.converters.Blob(e,{strict:!1});if(ArrayBuffer.isView(e)||k.isArrayBuffer(e))return A.converters.BufferSource(e)}return A.converters.USVString(e)},zr={WebSocket:S}}function vn(){if(_r)return Xr;return _r=1,Xr={isValidLastEventId:
/**
	 * Checks if the given value is a valid LastEventId.
	 * @param {string} value
	 * @returns {boolean}
	 */
function(A){
// LastEventId should not contain U+0000 NULL
return-1===A.indexOf("\0")}
/**
	 * Checks if the given value is a base 10 digit.
	 * @param {string} value
	 * @returns {boolean}
	 */,isASCIINumber:function(A){if(0===A.length)return!1;for(let e=0;e<A.length;e++)if(A.charCodeAt(e)<48||A.charCodeAt(e)>57)return!1;return!0}
// https://github.com/nodejs/undici/issues/2664
,delay:function(A){return new Promise(e=>{setTimeout(e,A).unref()})}}}function Jn(){if(en)return An;en=1;const{pipeline:A}=I,{fetching:e}=wn(),{makeRequest:t}=pn(),{webidl:r}=Ne(),{EventSourceStream:n}=function(){if($r)return jr;$r=1;const{Transform:A}=I,{isASCIINumber:e,isValidLastEventId:t}=vn(),r=[239,187,191],n=Buffer.from("data"),s=Buffer.from("event"),o=Buffer.from("id"),i=Buffer.from("retry");function a(A,e,t){if(e!==t.length)return!1;for(let r=0;r<e;r++)if(A[r]!==t[r])return!1;return!0}
/**
	 * @typedef {object} EventSourceStreamEvent
	 * @type {object}
	 * @property {string} [event] The event type.
	 * @property {string} [data] The data of the message.
	 * @property {string} [id] A unique ID for the event.
	 * @property {string} [retry] The reconnection time, in milliseconds.
	 */
/**
	 * @typedef eventSourceSettings
	 * @type {object}
	 * @property {string} lastEventId The last event ID received from the server.
	 * @property {string} origin The origin of the event source.
	 * @property {number} reconnectionTime The reconnection time, in milliseconds.
	 */return jr={EventSourceStream:class extends A{
/**
	   * @type {eventSourceSettings}
	   */
state=null
/**
	   * Leading byte-order-mark check.
	   * @type {boolean}
	   */;checkBOM=!0
/**
	   * @type {boolean}
	   */;crlfCheck=!1
/**
	   * @type {boolean}
	   */;eventEndCheck=!1
/**
	   * @type {Buffer[]}
	   */;chunks=[];chunkIndex=0;pos=0;lineChunkIndex=0;linePos=0;event={data:void 0,event:void 0,id:void 0,retry:void 0}
/**
	   * @param {object} options
	   * @param {eventSourceSettings} options.eventSourceSettings
	   * @param {Function} [options.push]
	   */;constructor(A={}){
// Enable object mode as EventSourceStream emits objects of shape
// EventSourceStreamEvent
A.readableObjectMode=!0,super(A),this.state=A.eventSourceSettings||{},A.push&&(this.push=A.push)}
/**
	   * @param {Buffer} chunk
	   * @param {string} _encoding
	   * @param {Function} callback
	   * @returns {void}
	   */_transform(A,e,t){if(0!==A.length)
// Strip leading byte-order-mark if we opened the stream and started
// the processing of the incoming data
if(this.chunks.push(A),this.checkBOM&&this.handleBOM())t();else{for(;this.hasCurrentByte();){const A=this.currentByte();
// If the previous line ended with an end-of-line, we need to check
// if the next character is also an end-of-line.
if(this.eventEndCheck){
// If the the current character is an end-of-line, then the event
// is finished and we can process it
// If the previous line ended with a carriage return, we need to
// check if the current character is a line feed and remove it
// from the buffer.
if(this.crlfCheck){
// If the current character is a line feed, we can remove it
// from the buffer and reset the crlfCheck flag
if(10===A){this.crlfCheck=!1,this.consumeCurrentByte();
// It is possible that the line feed is not the end of the
// event. We need to check if the next character is an
// end-of-line character to determine if the event is
// finished. We simply continue the loop to check the next
// character.
// As we removed the line feed from the buffer and set the
// crlfCheck flag to false, we basically don't make any
// distinction between a line feed and a carriage return.
continue}this.crlfCheck=!1}if(10===A||13===A){
// If the current character is a carriage return, we need to
// set the crlfCheck flag to true, as we need to check if the
// next character is a line feed so we can remove it from the
// buffer
13===A&&(this.crlfCheck=!0),this.consumeCurrentByte(),this.hasPendingEvent()&&this.processEvent(this.event),this.clearEvent();continue}
// If the current character is not an end-of-line, then the event
// is not finished and we have to reset the eventEndCheck flag
this.eventEndCheck=!1}
// If the current character is an end-of-line, we can process the
// line
else 10!==A&&13!==A?this.advanceCursor():(
// If the current character is a carriage return, we need to
// set the crlfCheck flag to true, as we need to check if the
// next character is a line feed
13===A&&(this.crlfCheck=!0),
// In any case, we can process the line as we reached an
// end-of-line character
this.parseLine(this.readLine(),this.event),this.consumeCurrentByte(),
// A line was processed and this could be the end of the event. We need
// to check if the next line is empty to determine if the event is
// finished.
this.eventEndCheck=!0)}t()}else t()}
/**
	   * @param {Buffer} line
	   * @param {EventStreamEvent} event
	   */parseLine(A,e){
// If the line is empty (a blank line)
// Dispatch the event, as defined below.
// This will be handled in the _transform method
if(0===A.length)return;
// If the line starts with a U+003A COLON character (:)
// Ignore the line.
const t=A.indexOf(58);if(0===t)return;let r=A.length,g=A.length;if(
// If the line contains a U+003A COLON character (:)
-1!==t&&(r=t,
// Collect the characters on the line after the first U+003A COLON
// character (:), and let value be that string.
// If value starts with a U+0020 SPACE character, remove it from value.
g=t+1,32===A[g]&&++g),a(A,r,n)){const t=A.toString("utf8",g);return void(void 0===e.data?e.data=t:e.data+=`\n${t}`)}if(a(A,r,i))(function(A,e){if(e>=A.length)return!1;for(let t=e;t<A.length;t++)if(A[t]<48||A[t]>57)return!1;return!0})(A,g)&&(e.retry=A.toString("utf8",g));else if(a(A,r,o))(function(A,e){for(let t=e;t<A.length;t++)if(0===A[t])return!1;return!0})(A,g)&&(e.id=A.toString("utf8",g));else if(a(A,r,s)){const t=A.toString("utf8",g);t.length>0&&(e.event=t)}}
/**
	   * @param {EventSourceStreamEvent} event
	   */processEvent(A){A.retry&&e(A.retry)&&(this.state.reconnectionTime=parseInt(A.retry,10)),A.id&&t(A.id)&&(this.state.lastEventId=A.id),
// only dispatch event, when data is provided
void 0!==A.data&&this.push({type:A.event||"message",options:{data:A.data,lastEventId:this.state.lastEventId,origin:this.state.origin}})}clearEvent(){this.event.data=void 0,this.event.event=void 0,this.event.id=void 0,this.event.retry=void 0}hasPendingEvent(){return void 0!==this.event.data||void 0!==this.event.event||void 0!==this.event.id||void 0!==this.event.retry}hasCurrentByte(){return this.chunkIndex<this.chunks.length&&this.pos<this.chunks[this.chunkIndex].length}currentByte(){return this.chunks[this.chunkIndex][this.pos]}consumeCurrentByte(){this.advanceCursor(),this.syncLineStartToCursor()}advanceCursor(){for(this.pos++;this.chunkIndex<this.chunks.length&&this.pos>=this.chunks[this.chunkIndex].length;)this.chunkIndex++,this.pos=0}syncLineStartToCursor(){this.lineChunkIndex=this.chunkIndex,this.linePos=this.pos,this.dropConsumedChunks()}dropConsumedChunks(){for(;this.lineChunkIndex>0;)this.chunks.shift(),this.lineChunkIndex--,this.chunkIndex--;this.chunkIndex===this.chunks.length&&(this.chunks.length=0,this.chunkIndex=0,this.pos=0,this.lineChunkIndex=0,this.linePos=0)}readLine(){if(this.lineChunkIndex===this.chunkIndex)return this.chunks[this.chunkIndex].subarray(this.linePos,this.pos);const A=[];let e=0;for(let t=this.lineChunkIndex;t<=this.chunkIndex;t++){const r=this.chunks[t],n=t===this.lineChunkIndex?this.linePos:0,s=t===this.chunkIndex?this.pos:r.length,o=r.subarray(n,s);e+=o.length,A.push(o)}return Buffer.concat(A,e)}peekBufferedByte(A){let e=this.lineChunkIndex,t=this.linePos;for(;e<this.chunks.length;){const r=this.chunks[e],n=r.length-t;if(A<n)return r[t+A];A-=n,e++,t=0}}discardLeadingBytes(A){for(;A>0&&this.lineChunkIndex<this.chunks.length;){const e=this.chunks[this.lineChunkIndex].length-this.linePos;A<e?(this.linePos+=A,A=0):(A-=e,this.lineChunkIndex++,this.linePos=0)}this.chunkIndex=this.lineChunkIndex,this.pos=this.linePos,this.dropConsumedChunks()}handleBOM(){const A=this.peekBufferedByte(0),e=this.peekBufferedByte(1),t=this.peekBufferedByte(2);return void 0===e?(A===r[0]||(this.checkBOM=!1),!0):void 0===t?A===r[0]&&e===r[1]||(this.checkBOM=!1,!1):(A===r[0]&&e===r[1]&&t===r[2]&&this.discardLeadingBytes(3),this.checkBOM=!1,!this.hasCurrentByte())}}}}(),{parseMIMEType:s}=Se(),{createFastMessageEvent:o}=Fn(),{isNetworkError:i}=dn(),{delay:a}=vn(),{kEnumerableProperty:g}=CA(),{environmentSettingsObject:c}=Me();let Q=!1;
/**
	 * A reconnection time, in milliseconds. This must initially be an implementation-defined value,
	 * probably in the region of a few seconds.
	 *
	 * In Comparison:
	 * - Chrome uses 3000ms.
	 * - Deno uses 5000ms.
	 *
	 * @type {3000}
	 */
/**
	 * The EventSource interface is used to receive server-sent events. It
	 * connects to a server over HTTP and receives events in text/event-stream
	 * format without closing the connection.
	 * @extends {EventTarget}
	 * @see https://html.spec.whatwg.org/multipage/server-sent-events.html#server-sent-events
	 * @api public
	 */
class E extends EventTarget{#X={open:null,error:null,message:null};#eA=null;#tA=!1;#rA=0;#nA=null;#sA=null;#A;
/**
	   * @type {import('./eventsource-stream').eventSourceSettings}
	   */#y;
/**
	   * Creates a new EventSource object.
	   * @param {string} url
	   * @param {EventSourceInit} [eventSourceInitDict]
	   * @see https://html.spec.whatwg.org/multipage/server-sent-events.html#the-eventsource-interface
	   */constructor(A,e={}){
// 1. Let ev be a new EventSource object.
super(),r.util.markAsUncloneable(this);const n="EventSource constructor";r.argumentLengthCheck(arguments,1,n),Q||(Q=!0,process.emitWarning("EventSource is experimental, expect them to change at any time.",{code:"UNDICI-ES"})),A=r.converters.USVString(A,n,"url"),e=r.converters.EventSourceInitDict(e,n,"eventSourceInitDict"),this.#A=e.dispatcher,this.#y={lastEventId:"",reconnectionTime:3e3};
// 2. Let settings be ev's relevant settings object.
// https://html.spec.whatwg.org/multipage/webappapis.html#environment-settings-object
const s=c;let o;try{
// 3. Let urlRecord be the result of encoding-parsing a URL given url, relative to settings.
o=new URL(A,s.settingsObject.baseUrl),this.#y.origin=o.origin}catch(A){
// 4. If urlRecord is failure, then throw a "SyntaxError" DOMException.
throw new DOMException(A,"SyntaxError")}
// 5. Set ev's url to urlRecord.
this.#eA=o.href;
// 6. Let corsAttributeState be Anonymous.
let i="anonymous";
// 7. If the value of eventSourceInitDict's withCredentials member is true,
// then set corsAttributeState to Use Credentials and set ev's
// withCredentials attribute to true.
e.withCredentials&&(i="use-credentials",this.#tA=!0);
// 8. Let request be the result of creating a potential-CORS request given
// urlRecord, the empty string, and corsAttributeState.
const a={redirect:"follow",keepalive:!0,
// @see https://html.spec.whatwg.org/multipage/urls-and-fetching.html#cors-settings-attributes
mode:"cors",credentials:"anonymous"===i?"same-origin":"omit",referrer:"no-referrer"};
// 9. Set request's client to settings.
a.client=c.settingsObject,
// 10. User agents may set (`Accept`, `text/event-stream`) in request's header list.
a.headersList=[["accept",{name:"accept",value:"text/event-stream"}]],
// 11. Set request's cache mode to "no-store".
a.cache="no-store",
// 12. Set request's initiator type to "other".
a.initiator="other",a.urlList=[new URL(this.#eA)],
// 13. Set ev's request to request.
this.#nA=t(a),this.#oA()}
/**
	   * Returns the state of this EventSource object's connection. It can have the
	   * values described below.
	   * @returns {0|1|2}
	   * @readonly
	   */get readyState(){return this.#rA}
/**
	   * Returns the URL providing the event stream.
	   * @readonly
	   * @returns {string}
	   */get url(){return this.#eA}
/**
	   * Returns a boolean indicating whether the EventSource object was
	   * instantiated with CORS credentials set (true), or not (false, the default).
	   */get withCredentials(){return this.#tA}#oA(){if(2===this.#rA)return;this.#rA=0;const t={request:this.#nA,dispatcher:this.#A};
// 14. Let processEventSourceEndOfBody given response res be the following step: if res is not a network error, then reestablish the connection.
// 15. Fetch request, with processResponseEndOfBody set to processEventSourceEndOfBody...
t.processResponseEndOfBody=A=>{i(A)&&(this.dispatchEvent(new Event("error")),this.close()),this.#iA()},
// and processResponse set to the following steps given response res:
t.processResponse=e=>{
// 1. If res is an aborted network error, then fail the connection.
if(i(e))
// 1. When a user agent is to fail the connection, the user agent
// must queue a task which, if the readyState attribute is set to a
// value other than CLOSED, sets the readyState attribute to CLOSED
// and fires an event named error at the EventSource object. Once the
// user agent has failed the connection, it does not attempt to
// reconnect.
return e.aborted?(this.close(),void this.dispatchEvent(new Event("error"))):void this.#iA();
// 3. Otherwise, if res's status is not 200, or if res's `Content-Type`
// is not `text/event-stream`, then fail the connection.
const t=e.headersList.get("content-type",!0),r=null!==t?s(t):"failure",a="failure"!==r&&"text/event-stream"===r.essence;if(200!==e.status||!1===a)return this.close(),void this.dispatchEvent(new Event("error"));
// 4. Otherwise, announce the connection and interpret res's body
// line by line.
// When a user agent is to announce the connection, the user agent
// must queue a task which, if the readyState attribute is set to a
// value other than CLOSED, sets the readyState attribute to OPEN
// and fires an event named open at the EventSource object.
// @see https://html.spec.whatwg.org/multipage/server-sent-events.html#sse-processing-model
this.#rA=1,this.dispatchEvent(new Event("open")),
// If redirected to a different origin, set the origin to the new origin.
this.#y.origin=e.urlList[e.urlList.length-1].origin;const g=new n({eventSourceSettings:this.#y,push:A=>{this.dispatchEvent(o(A.type,A.options))}});A(e.body.stream,g,A=>{!1===A?.aborted&&(this.close(),this.dispatchEvent(new Event("error")))})},this.#sA=e(t)}
/**
	   * @see https://html.spec.whatwg.org/multipage/server-sent-events.html#sse-processing-model
	   * @returns {Promise<void>}
	   */async#iA(){
// When a user agent is to reestablish the connection, the user agent must
// run the following steps. These steps are run in parallel, not as part of
// a task. (The tasks that it queues, of course, are run like normal tasks
// and not themselves in parallel.)
// 1. Queue a task to run the following steps:
//   1. If the readyState attribute is set to CLOSED, abort the task.
2!==this.#rA&&(
//   2. Set the readyState attribute to CONNECTING.
this.#rA=0,
//   3. Fire an event named error at the EventSource object.
this.dispatchEvent(new Event("error")),
// 2. Wait a delay equal to the reconnection time of the event source.
await a(this.#y.reconnectionTime),
// 5. Queue a task to run the following steps:
//   1. If the EventSource object's readyState attribute is not set to
//      CONNECTING, then return.
0===this.#rA&&(
//   2. Let request be the EventSource object's request.
//   3. If the EventSource object's last event ID string is not the empty
//      string, then:
//      1. Let lastEventIDValue be the EventSource object's last event ID
//         string, encoded as UTF-8.
//      2. Set (`Last-Event-ID`, lastEventIDValue) in request's header
//         list.
this.#y.lastEventId.length&&this.#nA.headersList.set("last-event-id",this.#y.lastEventId,!0),
//   4. Fetch request and process the response obtained in this fashion, if any, as described earlier in this section.
this.#oA()))}
/**
	   * Closes the connection, if any, and sets the readyState attribute to
	   * CLOSED.
	   */close(){r.brandCheck(this,E),2!==this.#rA&&(this.#rA=2,this.#sA.abort(),this.#nA=null)}get onopen(){return this.#X.open}set onopen(A){this.#X.open&&this.removeEventListener("open",this.#X.open),"function"==typeof A?(this.#X.open=A,this.addEventListener("open",A)):this.#X.open=null}get onmessage(){return this.#X.message}set onmessage(A){this.#X.message&&this.removeEventListener("message",this.#X.message),"function"==typeof A?(this.#X.message=A,this.addEventListener("message",A)):this.#X.message=null}get onerror(){return this.#X.error}set onerror(A){this.#X.error&&this.removeEventListener("error",this.#X.error),"function"==typeof A?(this.#X.error=A,this.addEventListener("error",A)):this.#X.error=null}}const B={CONNECTING:{__proto__:null,configurable:!1,enumerable:!0,value:0,writable:!1},OPEN:{__proto__:null,configurable:!1,enumerable:!0,value:1,writable:!1},CLOSED:{__proto__:null,configurable:!1,enumerable:!0,value:2,writable:!1}};return Object.defineProperties(E,B),Object.defineProperties(E.prototype,B),Object.defineProperties(E.prototype,{close:g,onerror:g,onmessage:g,onopen:g,readyState:g,url:g,withCredentials:g}),r.converters.EventSourceInitDict=r.dictionaryConverter([{key:"withCredentials",converter:r.converters.boolean,defaultValue:()=>!1},{key:"dispatcher",// undici only
converter:r.converters.any}]),An={EventSource:E,defaultReconnectionTime:3e3}}!function(){if(tn)return EA;tn=1;const A=Ve(),e=uA(),t=Oe(),r=function(){if(Be)return Ee;Be=1;const{BalancedPoolMissingUpstreamError:A,InvalidArgumentError:e}=hA(),{PoolBase:t,kClients:r,kNeedDrain:n,kAddClient:s,kRemoveClient:o,kGetDispatcher:i}=We(),a=Oe(),{kUrl:g,kInterceptors:c}=BA(),{parseOrigin:Q}=CA(),E=Symbol("factory"),B=Symbol("options"),h=Symbol("kGreatestCommonDivisor"),I=Symbol("kCurrentWeight"),C=Symbol("kIndex"),l=Symbol("kWeight"),u=Symbol("kMaxWeightPerServer"),d=Symbol("kErrorPenalty");
/**
	 * Calculate the greatest common divisor of two numbers by
	 * using the Euclidean algorithm.
	 *
	 * @param {number} a
	 * @param {number} b
	 * @returns {number}
	 */
function f(A,e){if(0===A)return e;for(;0!==e;){const t=e;e=A%e,A=t}return A}function p(A,e){return new a(A,e)}return Ee=class extends t{constructor(A=[],{factory:t=p,...r}={}){if(super(),this[B]=r,this[C]=-1,this[I]=0,this[u]=this[B].maxWeightPerServer||100,this[d]=this[B].errorPenalty||15,Array.isArray(A)||(A=[A]),"function"!=typeof t)throw new e("factory must be a function.");this[c]=r.interceptors?.BalancedPool&&Array.isArray(r.interceptors.BalancedPool)?r.interceptors.BalancedPool:[],this[E]=t;for(const e of A)this.addUpstream(e);this._updateBalancedPoolStats()}addUpstream(A){const e=Q(A).origin;if(this[r].find(A=>A[g].origin===e&&!0!==A.closed&&!0!==A.destroyed))return this;const t=this[E](e,Object.assign({},this[B]));this[s](t),t.on("connect",()=>{t[l]=Math.min(this[u],t[l]+this[d])}),t.on("connectionError",()=>{t[l]=Math.max(1,t[l]-this[d]),this._updateBalancedPoolStats()}),t.on("disconnect",(...A)=>{const e=A[2];e&&"UND_ERR_SOCKET"===e.code&&(
// decrease the weight of the pool.
t[l]=Math.max(1,t[l]-this[d]),this._updateBalancedPoolStats())});for(const A of this[r])A[l]=this[u];return this._updateBalancedPoolStats(),this}_updateBalancedPoolStats(){let A=0;for(let e=0;e<this[r].length;e++)A=f(this[r][e][l],A);this[h]=A}removeUpstream(A){const e=Q(A).origin,t=this[r].find(A=>A[g].origin===e&&!0!==A.closed&&!0!==A.destroyed);return t&&this[o](t),this}get upstreams(){return this[r].filter(A=>!0!==A.closed&&!0!==A.destroyed).map(A=>A[g].origin)}[i](){
// We validate that pools is greater than 0,
// otherwise we would have to wait until an upstream
// is added, which might never happen.
if(0===this[r].length)throw new A;if(!this[r].find(A=>!A[n]&&!0!==A.closed&&!0!==A.destroyed))return;const e=this[r].map(A=>A[n]).reduce((A,e)=>A&&e,!0);if(e)return;let t=0,s=this[r].findIndex(A=>!A[n]);for(;t++<this[r].length;){this[C]=(this[C]+1)%this[r].length;const A=this[r][this[C]];
// find pool index with the largest weight
if(A[l]>this[r][s][l]&&!A[n]&&(s=this[C]),
// decrease the current weight every `this[kClients].length`.
0===this[C]&&(
// Set the current weight to the next lower weight.
this[I]=this[I]-this[h],this[I]<=0&&(this[I]=this[u])),A[l]>=this[I]&&!A[n])return A}return this[I]=this[r][s][l],this[C]=s,this[r][s]}},Ee}(),n=Pe(),s=qe(),o=function(){if(de)return ue;de=1;const A=dA(),{kClose:e,kDestroy:t,kClosed:r,kDestroyed:n,kDispatch:s,kNoProxyAgent:o,kHttpProxyAgent:i,kHttpsProxyAgent:a}=BA(),g=qe(),c=Pe(),Q={"http:":80,"https:":443};let E=!1;return ue=class extends A{#aA=null;#gA=null;#D=null;constructor(A={}){super(),this.#D=A,E||(E=!0,process.emitWarning("EnvHttpProxyAgent is experimental, expect them to change at any time.",{code:"UNDICI-EHPA"}));const{httpProxy:e,httpsProxy:t,noProxy:r,...n}=A;this[o]=new c(n);const s=e??process.env.http_proxy??process.env.HTTP_PROXY;this[i]=s?new g({...n,uri:s}):this[o];const Q=t??process.env.https_proxy??process.env.HTTPS_PROXY;this[a]=Q?new g({...n,uri:Q}):this[i],this.#cA()}[s](A,e){const t=new URL(A.origin);return this.#QA(t).dispatch(A,e)}async[e](){await this[o].close(),this[i][r]||await this[i].close(),this[a][r]||await this[a].close()}async[t](A){await this[o].destroy(A),this[i][n]||await this[i].destroy(A),this[a][n]||await this[a].destroy(A)}#QA(A){let{protocol:e,host:t,port:r}=A;
// Stripping ports in this way instead of using parsedUrl.hostname to make
// sure that the brackets around IPv6 addresses are kept.
return t=t.replace(/:\d*$/,"").toLowerCase(),r=Number.parseInt(r,10)||Q[e]||0,this.#EA(t,r)?"https:"===e?this[a]:this[i]:this[o]}#EA(A,e){if(this.#BA&&this.#cA(),0===this.#gA.length)return!0;// Always proxy if NO_PROXY is not set or empty.
if("*"===this.#aA)return!1;// Never proxy if wildcard is set.
for(let t=0;t<this.#gA.length;t++){const r=this.#gA[t];if(!r.port||r.port===e)if(/^[.*]/.test(r.hostname)){
// Don't proxy if the hostname ends with the no_proxy host.
if(A.endsWith(r.hostname.replace(/^\*/,"")))return!1}else
// No wildcards, so don't proxy only if there is not an exact match.
if(A===r.hostname)return!1}return!0}#cA(){const A=this.#D.noProxy??this.#hA,e=A.split(/[,\s]/),t=[];for(let A=0;A<e.length;A++){const r=e[A];if(!r)continue;const n=r.match(/^(.+):(\d+)$/);t.push({hostname:(n?n[1]:r).toLowerCase(),port:n?Number.parseInt(n[2],10):0})}this.#aA=A,this.#gA=t}get#BA(){return void 0===this.#D.noProxy&&this.#aA!==this.#hA}get#hA(){return process.env.no_proxy??process.env.NO_PROXY??""}}}(),i=function(){if(ye)return we;ye=1;const A=uA(),e=Ze();return we=class extends A{#IA=null;#T=null;constructor(A,e={}){super(e),this.#IA=A,this.#T=e}dispatch(A,t){const r=new e({...A,retryOptions:this.#T},{dispatch:this.#IA.dispatch.bind(this.#IA),handler:t});return this.#IA.dispatch(A,r)}close(){return this.#IA.close()}destroy(){return this.#IA.destroy()}},we}(),a=hA(),g=CA(),{InvalidArgumentError:c}=a,Q=pt(),E=pA(),B=gn(),h=Qn(),I=cn(),l=wt(),u=Ze(),{getGlobalDispatcher:d,setGlobalDispatcher:f}=En(),p=Bn(),w=Je(),y=He();function D(A){return(e,t,r)=>{if("function"==typeof t&&(r=t,t=null),!e||"string"!=typeof e&&"object"!=typeof e&&!(e instanceof URL))throw new c("invalid url");if(null!=t&&"object"!=typeof t)throw new c("invalid opts");if(t&&null!=t.path){if("string"!=typeof t.path)throw new c("invalid opts.path");let A=t.path;t.path.startsWith("/")||(A=`/${A}`),e=new URL(g.parseOrigin(e).origin+A)}else t||(t="object"==typeof e?e:{}),e=g.parseURL(e);const{agent:n,dispatcher:s=d()}=t;if(n)throw new c("unsupported opts.agent. Did you mean opts.client?");return A.call(s,{...t,origin:e.origin,path:e.search?`${e.pathname}${e.search}`:e.pathname,method:t.method||(t.body?"PUT":"GET")},r)}}Object.assign(e.prototype,Q),EA.Dispatcher=e,EA.Client=A,EA.Pool=t,EA.BalancedPool=r,EA.Agent=n,EA.ProxyAgent=s,EA.EnvHttpProxyAgent=o,EA.RetryAgent=i,EA.RetryHandler=u,EA.DecoratorHandler=p,EA.RedirectHandler=w,EA.createRedirectInterceptor=y,EA.interceptors={redirect:hn(),retry:In(),dump:Cn(),dns:ln()},EA.buildConnector=E,EA.errors=a,EA.util={parseHeaders:g.parseHeaders,headerNameToString:g.headerNameToString},EA.setGlobalDispatcher=f,EA.getGlobalDispatcher=d;const m=wn().fetch;EA.fetch=async function(A,e=void 0){try{return await m(A,e)}catch(A){throw A&&"object"==typeof A&&Error.captureStackTrace(A),A}},EA.Headers=un().Headers,EA.Response=dn().Response,EA.Request=pn().Request,EA.FormData=Te().FormData,EA.File=globalThis.File??C.File,EA.FileReader=function(){if(Br)return Er;Br=1;const{staticPropertyDescriptors:A,readOperation:e,fireAProgressEvent:t}=Dn(),{kState:r,kError:n,kResult:s,kEvents:o,kAborted:i}=yn(),{webidl:a}=Ne(),{kEnumerableProperty:g}=CA();class c extends EventTarget{constructor(){super(),this[r]="empty",this[s]=null,this[n]=null,this[o]={loadend:null,error:null,abort:null,load:null,progress:null,loadstart:null}}
/**
	   * @see https://w3c.github.io/FileAPI/#dfn-readAsArrayBuffer
	   * @param {import('buffer').Blob} blob
	   */readAsArrayBuffer(A){a.brandCheck(this,c),a.argumentLengthCheck(arguments,1,"FileReader.readAsArrayBuffer"),A=a.converters.Blob(A,{strict:!1}),
// The readAsArrayBuffer(blob) method, when invoked,
// must initiate a read operation for blob with ArrayBuffer.
e(this,A,"ArrayBuffer")}
/**
	   * @see https://w3c.github.io/FileAPI/#readAsBinaryString
	   * @param {import('buffer').Blob} blob
	   */readAsBinaryString(A){a.brandCheck(this,c),a.argumentLengthCheck(arguments,1,"FileReader.readAsBinaryString"),A=a.converters.Blob(A,{strict:!1}),
// The readAsBinaryString(blob) method, when invoked,
// must initiate a read operation for blob with BinaryString.
e(this,A,"BinaryString")}
/**
	   * @see https://w3c.github.io/FileAPI/#readAsDataText
	   * @param {import('buffer').Blob} blob
	   * @param {string?} encoding
	   */readAsText(A,t=void 0){a.brandCheck(this,c),a.argumentLengthCheck(arguments,1,"FileReader.readAsText"),A=a.converters.Blob(A,{strict:!1}),void 0!==t&&(t=a.converters.DOMString(t,"FileReader.readAsText","encoding")),
// The readAsText(blob, encoding) method, when invoked,
// must initiate a read operation for blob with Text and encoding.
e(this,A,"Text",t)}
/**
	   * @see https://w3c.github.io/FileAPI/#dfn-readAsDataURL
	   * @param {import('buffer').Blob} blob
	   */readAsDataURL(A){a.brandCheck(this,c),a.argumentLengthCheck(arguments,1,"FileReader.readAsDataURL"),A=a.converters.Blob(A,{strict:!1}),
// The readAsDataURL(blob) method, when invoked, must
// initiate a read operation for blob with DataURL.
e(this,A,"DataURL")}
/**
	   * @see https://w3c.github.io/FileAPI/#dfn-abort
	   */abort(){
// 1. If this's state is "empty" or if this's state is
//    "done" set this's result to null and terminate
//    this algorithm.
"empty"!==this[r]&&"done"!==this[r]?(
// 2. If this's state is "loading" set this's state to
//    "done" and set this's result to null.
"loading"===this[r]&&(this[r]="done",this[s]=null),
// 3. If there are any tasks from this on the file reading
//    task source in an affiliated task queue, then remove
//    those tasks from that task queue.
this[i]=!0,
// 4. Terminate the algorithm for the read method being processed.
// TODO
// 5. Fire a progress event called abort at this.
t("abort",this),
// 6. If this's state is not "loading", fire a progress
//    event called loadend at this.
"loading"!==this[r]&&t("loadend",this)):this[s]=null}
/**
	   * @see https://w3c.github.io/FileAPI/#dom-filereader-readystate
	   */get readyState(){switch(a.brandCheck(this,c),this[r]){case"empty":return this.EMPTY;case"loading":return this.LOADING;case"done":return this.DONE}}
/**
	   * @see https://w3c.github.io/FileAPI/#dom-filereader-result
	   */get result(){
// The result attribute’s getter, when invoked, must return
// this's result.
return a.brandCheck(this,c),this[s]}
/**
	   * @see https://w3c.github.io/FileAPI/#dom-filereader-error
	   */get error(){
// The error attribute’s getter, when invoked, must return
// this's error.
return a.brandCheck(this,c),this[n]}get onloadend(){return a.brandCheck(this,c),this[o].loadend}set onloadend(A){a.brandCheck(this,c),this[o].loadend&&this.removeEventListener("loadend",this[o].loadend),"function"==typeof A?(this[o].loadend=A,this.addEventListener("loadend",A)):this[o].loadend=null}get onerror(){return a.brandCheck(this,c),this[o].error}set onerror(A){a.brandCheck(this,c),this[o].error&&this.removeEventListener("error",this[o].error),"function"==typeof A?(this[o].error=A,this.addEventListener("error",A)):this[o].error=null}get onloadstart(){return a.brandCheck(this,c),this[o].loadstart}set onloadstart(A){a.brandCheck(this,c),this[o].loadstart&&this.removeEventListener("loadstart",this[o].loadstart),"function"==typeof A?(this[o].loadstart=A,this.addEventListener("loadstart",A)):this[o].loadstart=null}get onprogress(){return a.brandCheck(this,c),this[o].progress}set onprogress(A){a.brandCheck(this,c),this[o].progress&&this.removeEventListener("progress",this[o].progress),"function"==typeof A?(this[o].progress=A,this.addEventListener("progress",A)):this[o].progress=null}get onload(){return a.brandCheck(this,c),this[o].load}set onload(A){a.brandCheck(this,c),this[o].load&&this.removeEventListener("load",this[o].load),"function"==typeof A?(this[o].load=A,this.addEventListener("load",A)):this[o].load=null}get onabort(){return a.brandCheck(this,c),this[o].abort}set onabort(A){a.brandCheck(this,c),this[o].abort&&this.removeEventListener("abort",this[o].abort),"function"==typeof A?(this[o].abort=A,this.addEventListener("abort",A)):this[o].abort=null}}
// https://w3c.github.io/FileAPI/#dom-filereader-empty
return c.EMPTY=c.prototype.EMPTY=0,
// https://w3c.github.io/FileAPI/#dom-filereader-loading
c.LOADING=c.prototype.LOADING=1,
// https://w3c.github.io/FileAPI/#dom-filereader-done
c.DONE=c.prototype.DONE=2,Object.defineProperties(c.prototype,{EMPTY:A,LOADING:A,DONE:A,readAsArrayBuffer:g,readAsBinaryString:g,readAsText:g,readAsDataURL:g,abort:g,readyState:g,result:g,error:g,onloadstart:g,onprogress:g,onload:g,onabort:g,onerror:g,onloadend:g,[Symbol.toStringTag]:{value:"FileReader",writable:!1,enumerable:!1,configurable:!0}}),Object.defineProperties(c,{EMPTY:A,LOADING:A,DONE:A}),Er={FileReader:c}}().FileReader;const{setGlobalOrigin:k,getGlobalOrigin:R}=Fe();EA.setGlobalOrigin=k,EA.getGlobalOrigin=R;const{CacheStorage:b}=function(){if(pr)return fr;pr=1;const{kConstruct:A}=mn(),{Cache:e}=kn(),{webidl:t}=Ne(),{kEnumerableProperty:r}=CA();class n{
/**
	   * @see https://w3c.github.io/ServiceWorker/#dfn-relevant-name-to-cache-map
	   * @type {Map<string, import('./cache').requestResponseList}
	   */
#CA=new Map;constructor(){arguments[0]!==A&&t.illegalConstructor(),t.util.markAsUncloneable(this)}async match(r,s={}){
// 1.
if(t.brandCheck(this,n),t.argumentLengthCheck(arguments,1,"CacheStorage.match"),r=t.converters.RequestInfo(r),null!=(s=t.converters.MultiCacheQueryOptions(s)).cacheName){
// 1.1.1.1
if(this.#CA.has(s.cacheName)){
// 1.1.1.1.1
const t=this.#CA.get(s.cacheName),n=new e(A,t);return await n.match(r,s)}}else// 2.
// 2.2
for(const t of this.#CA.values()){const n=new e(A,t),o=await n.match(r,s);
// 2.2.1.2
if(void 0!==o)return o}}
/**
	   * @see https://w3c.github.io/ServiceWorker/#cache-storage-has
	   * @param {string} cacheName
	   * @returns {Promise<boolean>}
	   */async has(A){t.brandCheck(this,n);const e="CacheStorage.has";
// 2.1.1
// 2.2
return t.argumentLengthCheck(arguments,1,e),A=t.converters.DOMString(A,e,"cacheName"),this.#CA.has(A)}
/**
	   * @see https://w3c.github.io/ServiceWorker/#dom-cachestorage-open
	   * @param {string} cacheName
	   * @returns {Promise<Cache>}
	   */async open(r){t.brandCheck(this,n);const s="CacheStorage.open";
// 2.1
if(t.argumentLengthCheck(arguments,1,s),r=t.converters.DOMString(r,s,"cacheName"),this.#CA.has(r)){
// await caches.open('v1') !== await caches.open('v1')
// 2.1.1
const t=this.#CA.get(r);
// 2.1.1.1
return new e(A,t)}
// 2.2
const o=[];
// 2.3
// 2.4
return this.#CA.set(r,o),new e(A,o)}
/**
	   * @see https://w3c.github.io/ServiceWorker/#cache-storage-delete
	   * @param {string} cacheName
	   * @returns {Promise<boolean>}
	   */async delete(A){t.brandCheck(this,n);const e="CacheStorage.delete";return t.argumentLengthCheck(arguments,1,e),A=t.converters.DOMString(A,e,"cacheName"),this.#CA.delete(A)}
/**
	   * @see https://w3c.github.io/ServiceWorker/#cache-storage-keys
	   * @returns {Promise<string[]>}
	   */async keys(){
// 2.2
return t.brandCheck(this,n),[...this.#CA.keys()]}}return Object.defineProperties(n.prototype,{[Symbol.toStringTag]:{value:"CacheStorage",configurable:!0},match:r,has:r,open:r,delete:r,keys:r}),fr={CacheStorage:n}}(),{kConstruct:F}=mn();
// Cache & CacheStorage are tightly coupled with fetch. Even if it may run
// in an older version of Node, it doesn't have any use without fetch.
EA.caches=new b(F);const{deleteCookie:S,getCookies:N,getSetCookies:M,setCookie:U}=function(){if(Fr)return br;Fr=1;const{parseSetCookie:A}=bn(),{stringify:e}=Rn(),{webidl:t}=Ne(),{Headers:r}=un();
/**
	 * @param {Headers} headers
	 * @param {Cookie} cookie
	 * @returns {void}
	 */
function n(A,n){t.argumentLengthCheck(arguments,2,"setCookie"),t.brandCheck(A,r,{strict:!1}),n=t.converters.Cookie(n);const s=e(n);s&&A.append("Set-Cookie",s)}return t.converters.DeleteCookieAttributes=t.dictionaryConverter([{converter:t.nullableConverter(t.converters.DOMString),key:"path",defaultValue:()=>null},{converter:t.nullableConverter(t.converters.DOMString),key:"domain",defaultValue:()=>null}]),t.converters.Cookie=t.dictionaryConverter([{converter:t.converters.DOMString,key:"name"},{converter:t.converters.DOMString,key:"value"},{converter:t.nullableConverter(A=>"number"==typeof A?t.converters["unsigned long long"](A):new Date(A)),key:"expires",defaultValue:()=>null},{converter:t.nullableConverter(t.converters["long long"]),key:"maxAge",defaultValue:()=>null},{converter:t.nullableConverter(t.converters.DOMString),key:"domain",defaultValue:()=>null},{converter:t.nullableConverter(t.converters.DOMString),key:"path",defaultValue:()=>null},{converter:t.nullableConverter(t.converters.boolean),key:"secure",defaultValue:()=>null},{converter:t.nullableConverter(t.converters.boolean),key:"httpOnly",defaultValue:()=>null},{converter:t.converters.USVString,key:"sameSite",allowedValues:["Strict","Lax","None"]},{converter:t.sequenceConverter(t.converters.DOMString),key:"unparsed",defaultValue:()=>new Array(0)}]),br={getCookies:
/**
	 * @typedef {Object} Cookie
	 * @property {string} name
	 * @property {string} value
	 * @property {Date|number|undefined} expires
	 * @property {number|undefined} maxAge
	 * @property {string|undefined} domain
	 * @property {string|undefined} path
	 * @property {boolean|undefined} secure
	 * @property {boolean|undefined} httpOnly
	 * @property {'Strict'|'Lax'|'None'} sameSite
	 * @property {string[]} unparsed
	 */
/**
	 * @param {Headers} headers
	 * @returns {Record<string, string>}
	 */
function(A){t.argumentLengthCheck(arguments,1,"getCookies"),t.brandCheck(A,r,{strict:!1});const e=A.get("cookie"),n={};if(!e)return n;for(const A of e.split(";")){const[e,...t]=A.split("=");n[e.trim()]=t.join("=")}return n}
/**
	 * @param {Headers} headers
	 * @param {string} name
	 * @param {{ path?: string, domain?: string }|undefined} attributes
	 * @returns {void}
	 */,deleteCookie:function(A,e,s){t.brandCheck(A,r,{strict:!1});const o="deleteCookie";t.argumentLengthCheck(arguments,2,o),e=t.converters.DOMString(e,o,"name"),s=t.converters.DeleteCookieAttributes(s),
// Matches behavior of
// https://github.com/denoland/deno_std/blob/63827b16330b82489a04614027c33b7904e08be5/http/cookie.ts#L278
n(A,{name:e,value:"",expires:new Date(0),...s})}
/**
	 * @param {Headers} headers
	 * @returns {Cookie[]}
	 */,getSetCookies:function(e){t.argumentLengthCheck(arguments,1,"getSetCookies"),t.brandCheck(e,r,{strict:!1});const n=e.getSetCookie();return n?n.map(e=>A(e)):[]},setCookie:n},br}();EA.deleteCookie=S,EA.getCookies=N,EA.getSetCookies=M,EA.setCookie=U;const{parseMIMEType:L,serializeAMimeType:T}=Se();EA.parseMIMEType=L,EA.serializeAMimeType=T;const{CloseEvent:G,ErrorEvent:Y,MessageEvent:v}=Fn();EA.WebSocket=Yn().WebSocket,EA.CloseEvent=G,EA.ErrorEvent=Y,EA.MessageEvent=v,EA.request=D(Q.request),EA.stream=D(Q.stream),EA.pipeline=D(Q.pipeline),EA.connect=D(Q.connect),EA.upgrade=D(Q.upgrade),EA.MockClient=B,EA.MockPool=I,EA.MockAgent=h,EA.mockErrors=l;const{EventSource:J}=Jn();EA.EventSource=J}(),function(A){A[A.OK=200]="OK",A[A.MultipleChoices=300]="MultipleChoices",A[A.MovedPermanently=301]="MovedPermanently",A[A.ResourceMoved=302]="ResourceMoved",A[A.SeeOther=303]="SeeOther",A[A.NotModified=304]="NotModified",A[A.UseProxy=305]="UseProxy",A[A.SwitchProxy=306]="SwitchProxy",A[A.TemporaryRedirect=307]="TemporaryRedirect",A[A.PermanentRedirect=308]="PermanentRedirect",A[A.BadRequest=400]="BadRequest",A[A.Unauthorized=401]="Unauthorized",A[A.PaymentRequired=402]="PaymentRequired",A[A.Forbidden=403]="Forbidden",A[A.NotFound=404]="NotFound",A[A.MethodNotAllowed=405]="MethodNotAllowed",A[A.NotAcceptable=406]="NotAcceptable",A[A.ProxyAuthenticationRequired=407]="ProxyAuthenticationRequired",A[A.RequestTimeout=408]="RequestTimeout",A[A.Conflict=409]="Conflict",A[A.Gone=410]="Gone",A[A.TooManyRequests=429]="TooManyRequests",A[A.InternalServerError=500]="InternalServerError",A[A.NotImplemented=501]="NotImplemented",A[A.BadGateway=502]="BadGateway",A[A.ServiceUnavailable=503]="ServiceUnavailable",A[A.GatewayTimeout=504]="GatewayTimeout"}(rn||(rn={})),function(A){A.Accept="accept",A.ContentType="content-type"}(nn||(nn={})),function(A){A.ApplicationJson="application/json"}(sn||(sn={})),rn.MovedPermanently,rn.ResourceMoved,rn.SeeOther,rn.TemporaryRedirect,rn.PermanentRedirect,rn.BadGateway,rn.ServiceUnavailable,rn.GatewayTimeout;const{access:Hn,appendFile:Vn,writeFile:xn}=n,{chmod:Wn,copyFile:On,lstat:Pn,mkdir:qn,open:Zn,readdir:zn,rename:Kn,rm:Xn,rmdir:_n,stat:jn,symlink:$n,unlink:As}=r.promises;
/**
 * The code to exit an action
 */
var es;
/**
 * Gets the value of an input.
 * Unless trimWhitespace is set to false in InputOptions, the value is also trimmed.
 * Returns an empty string if the value is not defined.
 *
 * @param     name     name of the input to get
 * @param     options  optional. See InputOptions.
 * @returns   string
 */
function ts(A,e){return(process.env[`INPUT_${A.replace(/ /g,"_").toUpperCase()}`]||"").trim()}
/**
 * Sets the value of an output.
 *
 * @param     name     name of the output to set
 * @param     value    value to store. Non-string values will be converted to a string via JSON.stringify
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function rs(e,n){if(process.env.GITHUB_OUTPUT||"")return function(e,t){const n=process.env[`GITHUB_${e}`];if(!n)throw new Error(`Unable to find environment variable for file command ${e}`);if(!r.existsSync(n))throw new Error(`Missing file at path: ${n}`);r.appendFileSync(n,`${T(t)}${A.EOL}`,{encoding:"utf8"})}("OUTPUT",function(e,r){const n=`ghadelimiter_${t.randomUUID()}`,s=T(r);
// These should realistically never happen, but just in case someone finds a
// way to exploit uuid generation let's not allow keys or values that contain
// the delimiter.
if(e.includes(n))throw new Error(`Unexpected input: name should not contain the delimiter "${n}"`);if(s.includes(n))throw new Error(`Unexpected input: value should not contain the delimiter "${n}"`);return`${e}<<${n}${A.EOL}${s}${A.EOL}${n}`}(e,n));process.stdout.write(A.EOL),Y("set-output",{name:e},T(n))}
//-----------------------------------------------------------------------
// Results
//-----------------------------------------------------------------------
/**
 * Sets the action status to failed.
 * When the action exits it will be with an exit code of 1
 * @param message add error issue message
 */function ns(A){process.exitCode=es.Failure,os(A)}
/**
 * Writes debug message to user log
 * @param message debug message
 */function ss(A){Y("debug",{},A)}
/**
 * Adds an error issue
 * @param message error issue message. Errors will be converted to string via toString()
 * @param properties optional properties to add to the annotation.
 */function os(A,e={}){Y("error",G(e),A instanceof Error?A.toString():A)}
/**
 * Adds a warning issue
 * @param message warning issue message. Errors will be converted to string via toString()
 * @param properties optional properties to add to the annotation.
 */function is(A,e={}){Y("warning",G(e),A instanceof Error?A.toString():A)}
/**
 * Writes info to log with console.log.
 * @param message info message
 */function as(e){process.stdout.write(e+A.EOL)}
/**
 * Returns a copy with defaults filled in.
 */function gs(A){const e={followSymbolicLinks:!0,implicitDescendants:!0,matchDirectories:!0,omitBrokenSymbolicLinks:!0,excludeHiddenFiles:!1};return A&&("boolean"==typeof A.followSymbolicLinks&&(e.followSymbolicLinks=A.followSymbolicLinks,ss(`followSymbolicLinks '${e.followSymbolicLinks}'`)),"boolean"==typeof A.implicitDescendants&&(e.implicitDescendants=A.implicitDescendants,ss(`implicitDescendants '${e.implicitDescendants}'`)),"boolean"==typeof A.matchDirectories&&(e.matchDirectories=A.matchDirectories,ss(`matchDirectories '${e.matchDirectories}'`)),"boolean"==typeof A.omitBrokenSymbolicLinks&&(e.omitBrokenSymbolicLinks=A.omitBrokenSymbolicLinks,ss(`omitBrokenSymbolicLinks '${e.omitBrokenSymbolicLinks}'`)),"boolean"==typeof A.excludeHiddenFiles&&(e.excludeHiddenFiles=A.excludeHiddenFiles,ss(`excludeHiddenFiles '${e.excludeHiddenFiles}'`))),e}
// export const {open} = 'fs'
process.platform,r.constants.O_RDONLY,
/* eslint-disable @typescript-eslint/unbound-method */
process.platform,e.platform(),e.arch(),function(A){
/**
     * A code indicating that the action was successful
     */
A[A.Success=0]="Success",
/**
     * A code indicating that the action was a failure
     */
A[A.Failure=1]="Failure"}(es||(es={}));const cs="win32"===process.platform;
/**
 * Similar to path.dirname except normalizes the path separators and slightly better handling for Windows UNC paths.
 *
 * For example, on Linux/macOS:
 * - `/               => /`
 * - `/hello          => /`
 *
 * For example, on Windows:
 * - `C:\             => C:\`
 * - `C:\hello        => C:\`
 * - `C:              => C:`
 * - `C:hello         => C:`
 * - `\               => \`
 * - `\hello          => \`
 * - `\\hello         => \\hello`
 * - `\\hello\world   => \\hello\world`
 */function Qs(A){
// Windows UNC root, e.g. \\hello or \\hello\world
if(
// Normalize slashes and trim unnecessary trailing slash
A=Cs(A),cs&&/^\\\\[^\\]+(\\[^\\]+)?$/.test(A))return A;
// Get dirname
let e=s.dirname(A);
// Trim trailing slash for Windows UNC root, e.g. \\hello\world\
return cs&&/^\\\\[^\\]+\\[^\\]+\\$/.test(e)&&(e=Cs(e)),e}
/**
 * Roots the path if not already rooted. On Windows, relative roots like `\`
 * or `C:` are expanded based on the current working directory.
 */function Es(A,e){
// Already rooted
if(c(A,"ensureAbsoluteRoot parameter 'root' must not be empty"),c(e,"ensureAbsoluteRoot parameter 'itemPath' must not be empty"),Bs(e))return e;
// Windows
if(cs){
// Check for itemPath like C: or C:foo
if(e.match(/^[A-Z]:[^\\/]|^[A-Z]:$/i)){let A=process.cwd();
// Drive letter matches cwd? Expand to cwd
return c(A.match(/^[A-Z]:\\/i),`Expected current directory to start with an absolute drive root. Actual '${A}'`),e[0].toUpperCase()===A[0].toUpperCase()?
// Drive only, e.g. C:
2===e.length?`${e[0]}:\\${A.substr(3)}`:(A.endsWith("\\")||(A+="\\"),`${e[0]}:\\${A.substr(3)}${e.substr(2)}`):`${e[0]}:\\${e.substr(2)}`}
// Check for itemPath like \ or \foo
if(Is(e).match(/^\\$|^\\[^\\]/)){const A=process.cwd();return c(A.match(/^[A-Z]:\\/i),`Expected current directory to start with an absolute drive root. Actual '${A}'`),`${A[0]}:\\${e.substr(1)}`}}return c(Bs(A),"ensureAbsoluteRoot parameter 'root' must have an absolute root"),
// Otherwise ensure root ends with a separator
A.endsWith("/")||cs&&A.endsWith("\\")||(
// Append separator
A+=s.sep),A+e}
/**
 * On Linux/macOS, true if path starts with `/`. On Windows, true for paths like:
 * `\\hello\share` and `C:\hello` (and using alternate separator).
 */function Bs(A){
// Windows
return c(A,"hasAbsoluteRoot parameter 'itemPath' must not be empty"),
// Normalize separators
A=Is(A),cs?A.startsWith("\\\\")||/^[A-Z]:\\/i.test(A):A.startsWith("/");
// E.g. /hello
}
/**
 * On Linux/macOS, true if path starts with `/`. On Windows, true for paths like:
 * `\`, `\hello`, `\\hello\share`, `C:`, and `C:\hello` (and using alternate separator).
 */function hs(A){
// Windows
return c(A,"isRooted parameter 'itemPath' must not be empty"),
// Normalize separators
A=Is(A),cs?A.startsWith("\\")||/^[A-Z]:/i.test(A):A.startsWith("/");
// E.g. /hello
}
/**
 * Removes redundant slashes and converts `/` to `\` on Windows
 */function Is(A){
// Windows
if(A=A||"",cs){
// Convert slashes on Windows
A=A.replace(/\//g,"\\");// e.g. \\hello
return(/^\\\\+[^\\]/.test(A)?"\\":"")+A.replace(/\\\\+/g,"\\");// preserve leading \\ for UNC
}
// Remove redundant slashes
return A.replace(/\/\/+/g,"/")}
/**
 * Normalizes the path separators and trims the trailing separator (when safe).
 * For example, `/foo/ => /foo` but `/ => /`
 */function Cs(A){
// Short-circuit if empty
return A?
// No trailing slash
(
// Normalize separators
A=Is(A)).endsWith(s.sep)?
// Check '/' on Linux/macOS and '\' on Windows
A===s.sep||cs&&/^[A-Z]:\\$/i.test(A)?A:A.substr(0,A.length-1):A:""}
/**
 * Indicates whether a pattern matches a path
 */var ls;!function(A){
/** Not matched */
A[A.None=0]="None",
/** Matched if the path is a directory */
A[A.Directory=1]="Directory",
/** Matched if the path is a regular file */
A[A.File=2]="File",
/** Matched */
A[A.All=3]="All"}(ls||(ls={}));const us="win32"===process.platform;
/**
 * Given an array of patterns, returns an array of paths to search.
 * Duplicates and paths under other included paths are filtered out.
 */function ds(A){
// Ignore negate patterns
A=A.filter(A=>!A.negate);
// Create a map of all search paths
const e={};for(const t of A){e[us?t.searchPath.toUpperCase():t.searchPath]="candidate"}const t=[];for(const r of A){
// Check if already included
const A=us?r.searchPath.toUpperCase():r.searchPath;if("included"===e[A])continue;
// Check for an ancestor search path
let n=!1,s=A,o=Qs(s);for(;o!==s;){if(e[o]){n=!0;break}s=o,o=Qs(s)}
// Include the search pattern in the result
n||(t.push(r.searchPath),e[A]="included")}return t}
/**
 * Matches the patterns against the path
 */function fs(A,e){let t=ls.None;for(const r of A)r.negate?t&=~r.match(e):t|=r.match(e);return t}
/**
 * Checks whether to descend further into the directory
 */function ps(A,e){return A.some(A=>!A.negate&&A.partialMatch(e))}const ws=(A,e,t)=>{const r=A instanceof RegExp?ys(A,t):A,n=e instanceof RegExp?ys(e,t):e,s=null!==r&&null!=n&&Ds(r,n,t);return s&&{start:s[0],end:s[1],pre:t.slice(0,s[0]),body:t.slice(s[0]+r.length,s[1]),post:t.slice(s[1]+n.length)}},ys=(A,e)=>{const t=e.match(A);return t?t[0]:null},Ds=(A,e,t)=>{let r,n,s,o,i,a=t.indexOf(A),g=t.indexOf(e,a+1),c=a;if(a>=0&&g>0){if(A===e)return[a,g];for(r=[],s=t.length;c>=0&&!i;){if(c===a)r.push(c),a=t.indexOf(A,c+1);else if(1===r.length){const A=r.pop();void 0!==A&&(i=[A,g])}else n=r.pop(),void 0!==n&&n<s&&(s=n,o=g),g=t.indexOf(e,c+1);c=a<g&&a>=0?a:g}r.length&&void 0!==o&&(i=[s,o])}return i},ms="\0SLASH"+Math.random()+"\0",ks="\0OPEN"+Math.random()+"\0",Rs="\0CLOSE"+Math.random()+"\0",bs="\0COMMA"+Math.random()+"\0",Fs="\0PERIOD"+Math.random()+"\0",Ss=new RegExp(ms,"g"),Ns=new RegExp(ks,"g"),Ms=new RegExp(Rs,"g"),Us=new RegExp(bs,"g"),Ls=new RegExp(Fs,"g"),Ts=/\\\\/g,Gs=/\\{/g,Ys=/\\}/g,vs=/\\,/g,Js=/\\\./g,Hs=1e5,Vs=4e6,xs=1e3,Ws=1e3;function Os(A){return isNaN(A)?A.charCodeAt(0):parseInt(A,10)}function Ps(A){return A.replace(Ss,"\\").replace(Ns,"{").replace(Ms,"}").replace(Us,",").replace(Ls,".")}
// Like `target.push(...items)` but doesn't overflow the stack
function qs(A,e){for(let t=0;t<e.length;t++)A.push(e[t])}
/**
 * Basically just str.split(","), but handling cases
 * where we have nested braced sections, which should be
 * treated as individual members, like {a,{b,c},d}
 */function Zs(A){const e=[];
// Walk the brace groups iteratively. Recursing on `post` once per group let a
// chain of them exhaust the stack - the parsing-side counterpart to
// the `expand_` overflow fixed for CVE-2026-14257, and not something `max` or
// `maxLength` can bound, since it happens before expansion.

// The part the next chunk continues
let t="";for(;;){const r=ws("{","}",A);if(!r){const r=A.split(",");return r[0]=t+r[0],qs(e,r),e}const{pre:n,body:s,post:o}=r,i=n.split(",");if(i[0]=t+i[0],i[i.length-1]+="{"+s+"}",!o.length)return qs(e,i),e;t=i.pop(),qs(e,i),A=o}}function zs(A,e={}){if(!A)return[];const{max:t=Hs,maxLength:r=Vs,maxDepth:n=xs,maxRewrites:s=Ws}=e;
// I don't know why Bash 4.3 does this, but it does.
// Anything starting with {} will have the first two bytes preserved
// but *only* at the top level, so {},a}b will not expand to anything,
// but a{},b}c will be expanded to [a}c,abc].
// One could argue that this is a bug in Bash, but since the goal of
// this module is to match Bash's rules, we escape a leading {}
return"{}"===A.slice(0,2)&&(A="\\{\\}"+A.slice(2)),eo(function(A){return A.replace(Ts,ms).replace(Gs,ks).replace(Ys,Rs).replace(vs,bs).replace(Js,Fs)}(A),t,r,n,0,s,!0).map(Ps)}function Ks(A){return"{"+A+"}"}function Xs(A){return/^-?0\d/.test(A)}function _s(A,e){return A<=e}function js(A,e){return A>=e}
// Build `{ acc[a] + pre + values[v] }` for every combination, capping the
// number of results at `max` and the total number of characters at `maxLength`.
// This is the one place output grows, so bounding it here keeps the single
// accumulator - and therefore memory - flat regardless of how many brace groups
// are combined (CVE-2026-14257).
function $s(A,e,t,r,n,s){const o=[];let i=0;for(let a=0;a<A.length;a++)for(let g=0;g<t.length;g++){if(o.length>=r)return o;const c=A[a]+e+t[g];
// Bash drops empty results at the top level. Skip them before they count
// against `max`, so `max` bounds the number of *kept* results.
if(!s||c){if(i+c.length>n)return o;o.push(c),i+=c.length}}return o}
// The expansion values of a single numeric (`1..5`) or alphabetic (`a..e..2`)
// sequence body.
function Ao(A,e,t,r){const n=A.split(/\.\./),s=[];
// A sequence body always splits into two or three parts, but the compiler
// can't know that.
/* c8 ignore start */
if(void 0===n[0]||void 0===n[1])return s;
/* c8 ignore stop */const o=Os(n[0]),i=Os(n[1]),a=Math.max(n[0].length,n[1].length);let g=3===n.length&&void 0!==n[2]?Math.max(Math.abs(Os(n[2])),1):1,c=_s;i<o&&(g*=-1,c=js);const Q=n.some(Xs);let E=0;for(let A=o;c(A,i)&&s.length<t;A+=g){let t;if(e)t=String.fromCharCode(A),"\\"===t&&(t="");else if(t=String(A),Q){const e=a-t.length;if(e>0){const r=new Array(e+1).join("0");t=A<0?"-"+r+t.slice(1):r+t}}if(E+t.length>r)break;s.push(t),E+=t.length}return s}function eo(A,e,t,r,n,s,o){
// Too deeply nested to keep following: treat the rest as literal, the same
// way a group that cannot expand is already handled. Truncating rather than
// throwing keeps `expand` total, matching `max` and `maxLength`.
if(n>r)return[A];
// Consume the string's top-level brace groups left to right, threading a
// running set of combined prefixes (`acc`). Expanding the tail iteratively -
// rather than recursing on `m.post` once per group - keeps the native stack
// depth constant, so deeply chained input (`'{a,b}'.repeat(3000)`) can no
// longer overflow the stack, and leaves a single accumulator whose size
// `maxLength` bounds directly (CVE-2026-14257).
let i=[""],a=0,g=!1,c=!0;
// Bash drops empty results, but only when the *first* top-level group is a
// comma set - a sequence like `{a..\}` may legitimately yield ''. The drop
// is on the final strings, so it is applied to whichever `combine` produces
// them (the one with no brace set left in the tail).
// How many times the `{a},b}` rewrite below has restarted the scan. Each pass
// re-reads the whole string, so leaving this unbounded is quadratic.
for(;;){const Q=ws("{","}",A);
// No brace set left: the rest of the string is literal.
if(!Q)return $s(i,A,[""],e,t,g);
// no need to expand pre, since it is guaranteed to be free of brace-sets
const E=Q.pre;if(/\$$/.test(E)){if(i=$s(i,E+"{"+Q.body+"}",[""],e,t,g&&!Q.post.length),c=!1,!Q.post.length)break;A=Q.post;continue}const B=/^-?\d+\.\.-?\d+(?:\.\.-?\d+)?$/.test(Q.body),h=/^[a-zA-Z]\.\.[a-zA-Z](?:\.\.-?\d+)?$/.test(Q.body),I=B||h,C=Q.body.indexOf(",")>=0;if(!I&&!C){
// {a},b}
if(a<s&&Q.post.match(/,(?!,).*\}/)){a++,A=Q.pre+"{"+Q.body+Rs+Q.post,o=!0;continue}
// Nothing here expands, so the whole remaining string is literal.
return $s(i,E+"{"+Q.body+"}"+Q.post,[""],e,t,g)}let l;if(c&&(g=o&&!I,c=!1),I)l=Ao(Q.body,h,e,t);else{let o=Zs(Q.body);if(1===o.length&&void 0!==o[0]&&(
// x{{a,b}}y ==> x{a}y x{b}y
o=eo(o[0],e,t,r,n+1,s,!1).map(Ks),1===o.length)){if(i=$s(i,E+o[0],[""],e,t,g&&!Q.post.length),!Q.post.length)break;A=Q.post;continue}
/* c8 ignore stop */
// Values that `combine` is going to drop as empty produce no result, so
// they must not count against `max` - otherwise `{a,,b}` with `max: 2`
// would stop at `['a', '']` and yield one result instead of two. Skipping
// them outright keeps `values` bounded while leaving `max` a bound on
// *kept* results.
let a=g&&!Q.post.length&&!E;for(let A=0;a&&A<i.length;A++)i[A]&&(a=!1);l=[];let c=0;A:for(let A=0;A<o.length;A++){const i=eo(o[A],e,t,r,n+1,s,!1);for(let A=0;A<i.length;A++){const r=i[A];if(!a||r){if(l.length>=e||c+r.length>t)break A;l.push(r),c+=r.length}}}}if(i=$s(i,E,l,e,t,g&&!Q.post.length),!Q.post.length)break;A=Q.post}return i}const to=A=>{if("string"!=typeof A)throw new TypeError("invalid pattern");if(A.length>65536)throw new TypeError("pattern is too long")},ro={"[:alnum:]":["\\p{L}\\p{Nl}\\p{Nd}",!0],"[:alpha:]":["\\p{L}\\p{Nl}",!0],"[:ascii:]":["\\x00-\\x7f",!1],"[:blank:]":["\\p{Zs}\\t",!0],"[:cntrl:]":["\\p{Cc}",!0],"[:digit:]":["\\p{Nd}",!0],"[:graph:]":["\\p{Z}\\p{C}",!0,!0],"[:lower:]":["\\p{Ll}",!0],"[:print:]":["\\p{C}",!0],"[:punct:]":["\\p{P}",!0],"[:space:]":["\\p{Z}\\t\\r\\n\\v\\f",!0],"[:upper:]":["\\p{Lu}",!0],"[:word:]":["\\p{L}\\p{Nl}\\p{Nd}\\p{Pc}",!0],"[:xdigit:]":["A-Fa-f0-9",!1]},no=A=>A.replace(/[[\]\\-]/g,"\\$&"),so=A=>A.join(""),oo=(A,e)=>{const t=e;
/* c8 ignore start */if("["!==A.charAt(t))throw new Error("not in a brace expression");
/* c8 ignore stop */const r=[],n=[];let s=t+1,o=!1,i=!1,a=!1,g=!1,c=t,Q="";A:for(;s<A.length;){const e=A.charAt(s);if("!"!==e&&"^"!==e||s!==t+1){if("]"===e&&o&&!a){c=s+1;break}if(o=!0,"\\"!==e||a){if("["===e&&!a)
// either a posix class, a collation equivalent, or just a [
for(const[e,[o,a,g]]of Object.entries(ro))if(A.startsWith(e,s)){
// invalid, [a-[] is fine, but not [a-[:alpha]]
if(Q)return["$.",!1,A.length-t,!0];s+=e.length,g?n.push(o):r.push(o),i=i||a;continue A}
// now it's just a normal character, effectively
a=!1,Q?(
// throw this range away if it's not valid, but others
// can still match.
e>Q?r.push(no(Q)+"-"+no(e)):e===Q&&r.push(no(e)),Q="",s++):
// now might be the start of a range.
// can be either c-d or c-] or c<more...>] or c] at this point
A.startsWith("-]",s+1)?(r.push(no(e+"-")),s+=2):A.startsWith("-",s+1)?(Q=e,s+=2):(
// not the start of a range, just a single character
r.push(no(e)),s++)}else a=!0,s++}else g=!0,s++}if(c<s)
// didn't see the end of the class, not a valid class,
// but might still be valid as a literal match.
return["",!1,0,!1];
// if we got no ranges and no negates, then we have a range that
// cannot possibly match anything, and that poisons the whole glob
if(!r.length&&!n.length)return["$.",!1,A.length-t,!0];
// if we got one positive range, and it's a single character, then that's
// not actually a magic pattern, it's just that one literal character.
// we should not treat that as "magic", we should just return the literal
// character. [_] is a perfectly valid way to escape glob magic chars.
if(0===n.length&&1===r.length&&/^\\?.$/.test(r[0])&&!g){const A=2===r[0].length?r[0].slice(-1):r[0];return[(E=A,E.replace(/[-[\]{}()*+?.,\\^$|#\s]/g,"\\$&")),!1,c-t,!1]}var E;const B="["+(g?"^":"")+so(r)+"]",h="["+(g?"":"^")+so(n)+"]";return[r.length&&n.length?"("+B+"|"+h+")":r.length?B:h,i,c-t,!0]},io=(A,{windowsPathsNoEscape:e=!1,magicalBraces:t=!0}={})=>t?e?A.replace(/\[([^/\\])\]/g,"$1"):A.replace(/((?!\\).|^)\[([^/\\])\]/g,"$1$2").replace(/\\([^/])/g,"$1"):e?A.replace(/\[([^/\\{}])\]/g,"$1"):A.replace(/((?!\\).|^)\[([^/\\{}])\]/g,"$1$2").replace(/\\([^/{}])/g,"$1");
// parse a single path portion
var ao;const go=new Set(["!","?","+","*","@"]),co=A=>go.has(A),Qo=A=>co(A.type),Eo=new Map([["!",["@"]],["?",["?","@"]],["@",["@"]],["*",["*","+","?","@"]],["+",["+","@"]]]),Bo=new Map([["!",["?"]],["@",["?"]],["+",["?","*"]]]),ho=new Map([["!",["?","@"]],["?",["?","@"]],["@",["?","@"]],["*",["*","+","?","@"]],["+",["+","@","?","*"]]]),Io=new Map([["!",new Map([["!","@"]])],["?",new Map([["*","*"],["+","*"]])],["@",new Map([["!","!"],["?","?"],["@","@"],["*","*"],["+","+"]])],["+",new Map([["?","*"],["*","*"]])]]),Co="(?!\\.)",lo=new Set(["[","."]),uo=new Set(["..","."]),fo=new Set("().*{}+?[]^$\\!"),po=A=>A.replace(/[-[\]{}()*+?.,\\^$|#\s]/g,"\\$&"),wo="[^/]",yo=wo+"*?",Do=wo+"+?";
// remove the \ chars that we added if we end up doing a nonmagic compare
// const deslash = (s: string) => s.replace(/\\(.)/g, '$1')
let mo=0;class ko{type;#lA;#uA;#dA=!1;#fA=[];#pA;#wA;#yA;#DA=!1;#T;#mA;
// set to true if it's an extglob with no children
// (which really means one child of '')
#kA=!1;id=++mo;get depth(){return(this.#pA?.depth??-1)+1}[Symbol.for("nodejs.util.inspect.custom")](){return{"@@type":"AST",id:this.id,type:this.type,root:this.#lA.id,parent:this.#pA?.id,depth:this.depth,partsLength:this.#fA.length,parts:this.#fA}}constructor(A,e,t={}){this.type=A,
// extglobs are inherently magical
A&&(this.#uA=!0),this.#pA=e,this.#lA=this.#pA?this.#pA.#lA:this,this.#T=this.#lA===this?t:this.#lA.#T,this.#yA=this.#lA===this?[]:this.#lA.#yA,"!"!==A||this.#lA.#DA||this.#yA.push(this),this.#wA=this.#pA?this.#pA.#fA.length:0}get hasMagic(){
/* c8 ignore start */
if(void 0!==this.#uA)return this.#uA;
/* c8 ignore stop */for(const A of this.#fA)if("string"!=typeof A&&(A.type||A.hasMagic))return this.#uA=!0;
// note: will be undefined until we generate the regexp src and find out
return this.#uA}
// reconstructs the pattern
toString(){return void 0!==this.#mA?this.#mA:this.type?this.#mA=this.type+"("+this.#fA.map(A=>String(A)).join("|")+")":this.#mA=this.#fA.map(A=>String(A)).join("")}#RA(){
/* c8 ignore start */
if(this!==this.#lA)throw new Error("should only call on root");if(this.#DA)return this;
/* c8 ignore stop */
// call toString() once to fill this out
let A;for(this.toString(),this.#DA=!0;A=this.#yA.pop();){if("!"!==A.type)continue;
// walk up the tree, appending everthing that comes AFTER parentIndex
let e=A,t=e.#pA;for(;t;){for(let r=e.#wA+1;!t.type&&r<t.#fA.length;r++)for(const e of A.#fA){
/* c8 ignore start */
if("string"==typeof e)throw new Error("string part in extglob AST??");
/* c8 ignore stop */e.copyIn(t.#fA[r])}e=t,t=e.#pA}}return this}push(...A){for(const e of A)if(""!==e){
/* c8 ignore start */
if("string"!=typeof e&&!(e instanceof ao&&e.#pA===this))throw new Error("invalid part: "+e);
/* c8 ignore stop */this.#fA.push(e)}}toJSON(){const A=null===this.type?this.#fA.slice().map(A=>"string"==typeof A?A:A.toJSON()):[this.type,...this.#fA.map(A=>A.toJSON())];return this.isStart()&&!this.type&&A.unshift([]),this.isEnd()&&(this===this.#lA||this.#lA.#DA&&"!"===this.#pA?.type)&&A.push({}),A}isStart(){if(this.#lA===this)return!0;
// if (this.type) return !!this.#parent?.isStart()
if(!this.#pA?.isStart())return!1;if(0===this.#wA)return!0;
// if everything AHEAD of this is a negation, then it's still the "start"
const A=this.#pA;for(let e=0;e<this.#wA;e++){const t=A.#fA[e];if(!(t instanceof ao&&"!"===t.type))return!1}return!0}isEnd(){if(this.#lA===this)return!0;if("!"===this.#pA?.type)return!0;if(!this.#pA?.isEnd())return!1;if(!this.type)return this.#pA?.isEnd();
// if not root, it'll always have a parent
/* c8 ignore start */const A=this.#pA?this.#pA.#fA.length:0;
/* c8 ignore stop */return this.#wA===A-1}copyIn(A){"string"==typeof A?this.push(A):this.push(A.clone(this))}clone(A){const e=new ao(this.type,A);for(const A of this.#fA)e.copyIn(A);return e}static#bA(A,e,t,r,n){const s=r.maxExtglobRecursion??2;let o=!1,i=!1,a=-1,g=!1;if(null===e.type){
// outside of a extglob, append until we find a start
let c=t,Q="";for(;c<A.length;){const t=A.charAt(c++);
// still accumulate escapes at this point, but we do ignore
// starts that are escaped
if(o||"\\"===t){o=!o,Q+=t;continue}if(i){c===a+1?"^"!==t&&"!"!==t||(g=!0):"]"!==t||c===a+2&&g||(i=!1),Q+=t;continue}
// we don't have to check for adoption here, because that's
// done at the other recursion point.
if("["===t){i=!0,a=c,g=!1,Q+=t;continue}if(!r.noext&&co(t)&&"("===A.charAt(c)&&n<=s){e.push(Q),Q="";const s=new ao(t,e);c=ao.#bA(A,s,c,r,n+1),e.push(s);continue}Q+=t}return e.push(Q),c}
// some kind of extglob, pos is at the (
// find the next | or )
let c=t+1,Q=new ao(null,e);const E=[];let B="";for(;c<A.length;){const t=A.charAt(c++);
// still accumulate escapes at this point, but we do ignore
// starts that are escaped
if(o||"\\"===t){o=!o,B+=t;continue}if(i){c===a+1?"^"!==t&&"!"!==t||(g=!0):"]"!==t||c===a+2&&g||(i=!1),B+=t;continue}if("["===t){i=!0,a=c,g=!1,B+=t;continue}
/* c8 ignore stop */
if(!r.noext&&co(t)&&"("===A.charAt(c)&&(
/* c8 ignore start - the maxDepth is sufficient here */
n<=s||e&&e.#FA(t))){const s=e&&e.#FA(t)?0:1;Q.push(B),B="";const o=new ao(t,Q);Q.push(o),c=ao.#bA(A,o,c,r,n+s);continue}if("|"!==t){if(")"===t)return""===B&&0===e.#fA.length&&(e.#kA=!0),Q.push(B),B="",e.push(...E,Q),c;B+=t}else Q.push(B),B="",E.push(Q),Q=new ao(null,e)}
// unfinished extglob
// if we got here, it was a malformed extglob! not an extglob, but
// maybe something else in there.
return e.type=null,e.#uA=void 0,e.#fA=[A.substring(t-1)],c}#SA(A){return this.#NA(A,Bo)}#NA(A,e=Eo){if(!A||"object"!=typeof A||null!==A.type||1!==A.#fA.length||null===this.type)return!1;const t=A.#fA[0];return!(!t||"object"!=typeof t||null===t.type)&&this.#FA(t.type,e)}#FA(A,e=ho){return!!e.get(this.type)?.includes(A)}#MA(A,e){const t=A.#fA[0],r=new ao(null,t,this.options);r.#fA.push(""),t.push(r),this.#UA(A,e)}#UA(A,e){const t=A.#fA[0];this.#fA.splice(e,1,...t.#fA);for(const A of t.#fA)"object"==typeof A&&(A.#pA=this);this.#mA=void 0}#LA(A){const e=Io.get(this.type);return!!e?.has(A)}#TA(A){if(!A||"object"!=typeof A||null!==A.type||1!==A.#fA.length||null===this.type||1!==this.#fA.length)return!1;const e=A.#fA[0];return!(!e||"object"!=typeof e||null===e.type)&&this.#LA(e.type)}#GA(A){const e=Io.get(this.type),t=A.#fA[0],r=e?.get(t.type);
/* c8 ignore start - impossible */
if(!r)return!1;
/* c8 ignore stop */this.#fA=t.#fA;for(const A of this.#fA)"object"==typeof A&&(A.#pA=this);this.type=r,this.#mA=void 0,this.#kA=!1}static fromGlob(A,e={}){const t=new ao(null,void 0,e);return ao.#bA(A,t,0,e,0),t}
// returns the regular expression if there's magic, or the unescaped
// string if not.
toMMPattern(){
// should only be called on root
/* c8 ignore start */
if(this!==this.#lA)return this.#lA.toMMPattern();
/* c8 ignore stop */const A=this.toString(),[e,t,r,n]=this.toRegExpSource();if(!(r||this.#uA||this.#T.nocase&&!this.#T.nocaseMagicOnly&&A.toUpperCase()!==A.toLowerCase()))return t;const s=(this.#T.nocase?"i":"")+(n?"u":"");return Object.assign(new RegExp(`^${e}$`,s),{_src:e,_glob:A})}get options(){return this.#T}
// returns the string match, the regexp source, whether there's magic
// in the regexp (so a regular expression is required) and whether or
// not the uflag is needed for the regular expression (for posix classes)
// TODO: instead of injecting the start/end at this point, just return
// the BODY of the regexp, along with the start/end portions suitable
// for binding the start/end in either a joined full-path makeRe context
// (where we bind to (^|/), or a standalone matchPart context (where
// we bind to ^, and not /).  Otherwise slashes get duped!
// In part-matching mode, the start is:
// - if not isStart: nothing
// - if traversal possible, but not allowed: ^(?!\.\.?$)
// - if dots allowed or not possible: ^
// - if dots possible and not allowed: ^(?!\.)
// end is:
// - if not isEnd(): nothing
// - else: $
// In full-path matching mode, we put the slash at the START of the
// pattern, so start is:
// - if first pattern: same as part-matching mode
// - if not isStart(): nothing
// - if traversal possible, but not allowed: /(?!\.\.?(?:$|/))
// - if dots allowed or not possible: /
// - if dots possible and not allowed: /(?!\.)
// end is:
// - if last pattern, same as part-matching mode
// - else nothing
// Always put the (?:$|/) on negated tails, though, because that has to be
// there to bind the end of the negated pattern portion, and it's easier to
// just stick it in now rather than try to inject it later in the middle of
// the pattern.
// We can just always return the same end, and leave it up to the caller
// to know whether it's going to be used joined or in parts.
// And, if the start is adjusted slightly, can do the same there:
// - if not isStart: nothing
// - if traversal possible, but not allowed: (?:/|^)(?!\.\.?$)
// - if dots allowed or not possible: (?:/|^)
// - if dots possible and not allowed: (?:/|^)(?!\.)
// But it's better to have a simpler binding without a conditional, for
// performance, so probably better to return both start options.
// Then the caller just ignores the end if it's not the first pattern,
// and the start always gets applied.
// But that's always going to be $ if it's the ending pattern, or nothing,
// so the caller can just attach $ at the end of the pattern when building.
// So the todo is:
// - better detect what kind of start is needed
// - return both flavors of starting pattern
// - attach $ at the end of the pattern when creating the actual RegExp
// Ah, but wait, no, that all only applies to the root when the first pattern
// is not an extglob. If the first pattern IS an extglob, then we need all
// that dot prevention biz to live in the extglob portions, because eg
// +(*|.x*) can match .xy but not .yx.
// So, return the two flavors if it's #root and the first child is not an
// AST, otherwise leave it to the child AST to handle it, and there,
// use the (?:^|/) style of start binding.
// Even simplified further:
// - Since the start for a join is eg /(?!\.) and the start for a part
// is ^(?!\.), we can just prepend (?!\.) to the pattern (either root
// or start or whatever) and prepend ^ or / at the Regexp construction.
toRegExpSource(A){const e=A??!!this.#T.dot;if(this.#lA===this&&(this.#YA(),this.#RA()),!Qo(this)){const t=this.isStart()&&this.isEnd()&&!this.#fA.some(A=>"string"!=typeof A),r=this.#fA.map(e=>{const[r,n,s,o]="string"==typeof e?ao.#vA(e,this.#uA,t):e.toRegExpSource(A);return this.#uA=this.#uA||s,this.#dA=this.#dA||o,r}).join("");let n="";if(this.isStart()&&"string"==typeof this.#fA[0]){if(!(1===this.#fA.length&&uo.has(this.#fA[0]))){const t=lo,s=
// dots are allowed, and the pattern starts with [ or .
e&&t.has(r.charAt(0))||
// the pattern starts with \., and then [ or .
r.startsWith("\\.")&&t.has(r.charAt(2))||
// the pattern starts with \.\., and then [ or .
r.startsWith("\\.\\.")&&t.has(r.charAt(4)),o=!e&&!A&&t.has(r.charAt(0));
// check if we have a possibility of matching . or ..,
// and prevent that.
n=s?"(?!(?:^|/)\\.\\.?(?:$|/))":o?Co:""}}
// append the "end of path portion" pattern to negation tails
let s="";this.isEnd()&&this.#lA.#DA&&"!"===this.#pA?.type&&(s="(?:$|\\/)");return[n+r+s,io(r),this.#uA=!!this.#uA,this.#dA]}
// We need to calculate the body *twice* if it's a repeat pattern
// at the start, once in nodot mode, then again in dot mode, so a
// pattern like *(?) can match 'x.y'
const t="*"===this.type||"+"===this.type,r="!"===this.type?"(?:(?!(?:":"(?:";
// some kind of extglob
let n=this.#JA(e);if(this.isStart()&&this.isEnd()&&!n&&"!"!==this.type){
// invalid extglob, has to at least be *something* present, if it's
// the entire path portion.
const A=this.toString(),e=this;return e.#fA=[A],e.type=null,e.#uA=void 0,[A,io(this.toString()),!1,!1]}let s=!t||A||e?"":this.#JA(!0);s===n&&(s=""),s&&(n=`(?:${n})(?:${s})*?`);
// an empty !() is exactly equivalent to a starNoEmpty
let o="";if("!"===this.type&&this.#kA)o=(this.isStart()&&!e?Co:"")+Do;else{o=r+n+("!"===this.type?
// !() must match something,but !(x) can match ''
"))"+(!this.isStart()||e||A?"":Co)+yo+")":"@"===this.type?")":"?"===this.type?")?":"+"===this.type&&s?")":"*"===this.type&&s?")?":`)${this.type}`)}return[o,io(n),this.#uA=!!this.#uA,this.#dA]}#YA(){if(Qo(this)){
// do up to 10 passes to flatten as much as possible
let A=0,e=!1;do{e=!0;for(let A=0;A<this.#fA.length;A++){const t=this.#fA[A];"object"==typeof t&&(t.#YA(),this.#NA(t)?(e=!1,this.#UA(t,A)):this.#SA(t)?(e=!1,this.#MA(t,A)):this.#TA(t)&&(e=!1,this.#GA(t)))}}while(!e&&++A<10)}else for(const A of this.#fA)"object"==typeof A&&A.#YA();this.#mA=void 0}#JA(A){return this.#fA.map(e=>{
// extglob ASTs should only contain parent ASTs
/* c8 ignore start */
if("string"==typeof e)throw new Error("string type in extglob ast??");
/* c8 ignore stop */
// can ignore hasMagic, because extglobs are already always magic
const[t,r,n,s]=e.toRegExpSource(A);return this.#dA=this.#dA||s,t}).filter(A=>!(this.isStart()&&this.isEnd()&&!A)).join("|")}static#vA(A,e,t=!1){let r=!1,n="",s=!1,o=!1;for(let i=0;i<A.length;i++){const a=A.charAt(i);if(r)r=!1,n+=(fo.has(a)?"\\":"")+a;else if("*"!==a)if(o=!1,"\\"!==a){if("["===a){const[t,r,o,a]=oo(A,i);if(o){n+=t,s=s||r,i+=o-1,e=e||a;continue}}"?"!==a?n+=po(a):(n+=wo,e=!0)}else i===A.length-1?n+="\\\\":r=!0;else{if(o)continue;o=!0,n+=t&&/^[*]+$/.test(A)?Do:yo,e=!0}}return[n,io(A),!!e,s]}}ao=ko;
/**
 * Escape all magic characters in a glob pattern.
 *
 * If the {@link MinimatchOptions.windowsPathsNoEscape}
 * option is used, then characters are escaped by wrapping in `[]`, because
 * a magic character wrapped in a character class can only be satisfied by
 * that exact character.  In this mode, `\` is _not_ escaped, because it is
 * not interpreted as a magic character, but instead as a path separator.
 *
 * If the {@link MinimatchOptions.magicalBraces} option is used,
 * then braces (`{` and `}`) will be escaped.
 */
const Ro=(A,e,t={})=>(to(e),
// shortcut: comments match nothing.
!(!t.nocomment&&"#"===e.charAt(0))&&new $o(e,t).match(A)),bo=/^\*+([^+@!?*[(]*)$/,Fo=A=>e=>!e.startsWith(".")&&e.endsWith(A),So=A=>e=>e.endsWith(A),No=A=>(A=A.toLowerCase(),e=>!e.startsWith(".")&&e.toLowerCase().endsWith(A)),Mo=A=>(A=A.toLowerCase(),e=>e.toLowerCase().endsWith(A)),Uo=/^\*+\.\*+$/,Lo=A=>!A.startsWith(".")&&A.includes("."),To=A=>"."!==A&&".."!==A&&A.includes("."),Go=/^\.\*+$/,Yo=A=>"."!==A&&".."!==A&&A.startsWith("."),vo=/^\*+$/,Jo=A=>0!==A.length&&!A.startsWith("."),Ho=A=>0!==A.length&&"."!==A&&".."!==A,Vo=/^\?+([^+@!?*[(]*)?$/,xo=([A,e=""])=>{const t=qo([A]);return e?(e=e.toLowerCase(),A=>t(A)&&A.toLowerCase().endsWith(e)):t},Wo=([A,e=""])=>{const t=Zo([A]);return e?(e=e.toLowerCase(),A=>t(A)&&A.toLowerCase().endsWith(e)):t},Oo=([A,e=""])=>{const t=Zo([A]);return e?A=>t(A)&&A.endsWith(e):t},Po=([A,e=""])=>{const t=qo([A]);return e?A=>t(A)&&A.endsWith(e):t},qo=([A])=>{const e=A.length;return A=>A.length===e&&!A.startsWith(".")},Zo=([A])=>{const e=A.length;return A=>A.length===e&&"."!==A&&".."!==A},zo="object"==typeof process&&process?"object"==typeof process.env&&process.env&&process.env.__MINIMATCH_TESTING_PLATFORM__||process.platform:"posix";Ro.sep="win32"===zo?{sep:"\\"}.sep:{sep:"/"}.sep;const Ko=Symbol("globstar **");Ro.GLOBSTAR=Ko;Ro.filter=(A,e={})=>t=>Ro(t,A,e);const Xo=(A,e={})=>Object.assign({},A,e);Ro.defaults=A=>{if(!A||"object"!=typeof A||!Object.keys(A).length)return Ro;const e=Ro;return Object.assign((t,r,n={})=>e(t,r,Xo(A,n)),{Minimatch:class extends e.Minimatch{constructor(e,t={}){super(e,Xo(A,t))}static defaults(t){return e.defaults(Xo(A,t)).Minimatch}},AST:class extends e.AST{
/* c8 ignore start */
constructor(e,t,r={}){super(e,t,Xo(A,r))}
/* c8 ignore stop */static fromGlob(t,r={}){return e.AST.fromGlob(t,Xo(A,r))}},unescape:(t,r={})=>e.unescape(t,Xo(A,r)),escape:(t,r={})=>e.escape(t,Xo(A,r)),filter:(t,r={})=>e.filter(t,Xo(A,r)),defaults:t=>e.defaults(Xo(A,t)),makeRe:(t,r={})=>e.makeRe(t,Xo(A,r)),braceExpand:(t,r={})=>e.braceExpand(t,Xo(A,r)),match:(t,r,n={})=>e.match(t,r,Xo(A,n)),sep:e.sep,GLOBSTAR:Ko})};
// Brace expansion:
// a{b,c}d -> abd acd
// a{b,}c -> abc ac
// a{0..3}d -> a0d a1d a2d a3d
// a{b,c{d,e}f}g -> abg acdfg acefg
// a{b,c}d{e,f}g -> abdeg acdeg abdeg abdfg
// Invalid sets are not expanded.
// a{2..}b -> a{2..}b
// a{b}c -> a{b}c
const _o=(A,e={})=>(to(A),
// Thanks to Yeting Li <https://github.com/yetingli> for
// improving this regexp to avoid a ReDOS vulnerability.
e.nobrace||!/\{(?:(?!\{).)*\}/.test(A)?[A]:zs(A,{max:e.braceExpandMax}));Ro.braceExpand=_o;Ro.makeRe=(A,e={})=>new $o(A,e).makeRe();Ro.match=(A,e,t={})=>{const r=new $o(e,t);return A=A.filter(A=>r.match(A)),r.options.nonull&&!A.length&&A.push(e),A};
// replace stuff like \* with *
const jo=/[?*]|[+@!]\(.*?\)|\[|\]/;class $o{options;set;pattern;windowsPathsNoEscape;nonegate;negate;comment;empty;preserveMultipleSlashes;partial;globSet;globParts;nocase;isWindows;platform;windowsNoMagicRoot;maxGlobstarRecursion;regexp;constructor(A,e={}){to(A),e=e||{},this.options=e,this.maxGlobstarRecursion=e.maxGlobstarRecursion??200,this.pattern=A,this.platform=e.platform||zo,this.isWindows="win32"===this.platform;this.windowsPathsNoEscape=!!e.windowsPathsNoEscape||!1===e.allowWindowsEscape,this.windowsPathsNoEscape&&(this.pattern=this.pattern.replace(/\\/g,"/")),this.preserveMultipleSlashes=!!e.preserveMultipleSlashes,this.regexp=null,this.negate=!1,this.nonegate=!!e.nonegate,this.comment=!1,this.empty=!1,this.partial=!!e.partial,this.nocase=!!this.options.nocase,this.windowsNoMagicRoot=void 0!==e.windowsNoMagicRoot?e.windowsNoMagicRoot:!(!this.isWindows||!this.nocase),this.globSet=[],this.globParts=[],this.set=[],
// make the set of regexps etc.
this.make()}hasMagic(){if(this.options.magicalBraces&&this.set.length>1)return!0;for(const A of this.set)for(const e of A)if("string"!=typeof e)return!0;return!1}debug(...A){}make(){const A=this.pattern,e=this.options;
// empty patterns and comments match nothing.
if(!e.nocomment&&"#"===A.charAt(0))return void(this.comment=!0);if(!A)return void(this.empty=!0);
// step 1: figure out negation, etc.
this.parseNegate(),
// step 2: expand braces
this.globSet=[...new Set(this.braceExpand())],e.debug&&(
//oxlint-disable-next-line no-console
this.debug=(...A)=>console.error(...A)),this.debug(this.pattern,this.globSet);
// step 3: now we have a set, so turn each one into a series of
// path-portion matching patterns.
// These will be regexps, except in the case of "**", which is
// set to the GLOBSTAR object for globstar behavior,
// and will not contain any / characters
// First, we preprocess to make the glob pattern sets a bit simpler
// and deduped.  There are some perf-killing patterns that can cause
// problems with a glob walk, but we can simplify them down a bit.
const t=this.globSet.map(A=>this.slashSplit(A));this.globParts=this.preprocess(t),this.debug(this.pattern,this.globParts);
// glob --> regexps
let r=this.globParts.map((A,e,t)=>{if(this.isWindows&&this.windowsNoMagicRoot){
// check if it's a drive or unc path.
const e=!(""!==A[0]||""!==A[1]||"?"!==A[2]&&jo.test(A[2])||jo.test(A[3])),t=/^[a-z]:/i.test(A[0]);if(e)return[...A.slice(0,4),...A.slice(4).map(A=>this.parse(A))];if(t)return[A[0],...A.slice(1).map(A=>this.parse(A))]}return A.map(A=>this.parse(A))});
// do not treat the ? in UNC paths as magic
if(this.debug(this.pattern,r),
// filter out everything that didn't compile properly.
this.set=r.filter(A=>-1===A.indexOf(!1)),this.isWindows)for(let A=0;A<this.set.length;A++){const e=this.set[A];""===e[0]&&""===e[1]&&"?"===this.globParts[A][2]&&"string"==typeof e[3]&&/^[a-z]:$/i.test(e[3])&&(e[2]="?")}this.debug(this.pattern,this.set)}
// various transforms to equivalent pattern sets that are
// faster to process in a filesystem walk.  The goal is to
// eliminate what we can, and push all ** patterns as far
// to the right as possible, even if it increases the number
// of patterns that we have to process.
preprocess(A){
// if we're not in globstar mode, then turn ** into *
if(this.options.noglobstar)for(const e of A)for(let A=0;A<e.length;A++)"**"===e[A]&&(e[A]="*");const{optimizationLevel:e=1}=this.options;return e>=2?(
// aggressive optimization for the purpose of fs walking
A=this.firstPhasePreProcess(A),A=this.secondPhasePreProcess(A)):
// just basic optimizations to remove some .. parts
A=e>=1?this.levelOneOptimize(A):this.adjascentGlobstarOptimize(A),A}
// just get rid of adjascent ** portions
adjascentGlobstarOptimize(A){return A.map(A=>{let e=-1;for(;-1!==(e=A.indexOf("**",e+1));){let t=e;for(;"**"===A[t+1];)t++;t!==e&&A.splice(e,t-e)}return A})}
// get rid of adjascent ** and resolve .. portions
levelOneOptimize(A){return A.map(A=>0===(A=A.reduce((A,e)=>{const t=A[A.length-1];return"**"===e&&"**"===t?A:".."===e&&t&&".."!==t&&"."!==t&&"**"!==t?(A.pop(),A):(A.push(e),A)},[])).length?[""]:A)}levelTwoFileOptimize(A){Array.isArray(A)||(A=this.slashSplit(A));let e=!1;do{
// <pre>/<e>/<rest> -> <pre>/<rest>
if(e=!1,!this.preserveMultipleSlashes){for(let t=1;t<A.length-1;t++){const r=A[t];
// don't squeeze out UNC patterns
1===t&&""===r&&""===A[0]||("."!==r&&""!==r||(e=!0,A.splice(t,1),t--))}"."!==A[0]||2!==A.length||"."!==A[1]&&""!==A[1]||(e=!0,A.pop())}
// <pre>/<p>/../<rest> -> <pre>/<rest>
let t=0;for(;-1!==(t=A.indexOf("..",t+1));){const r=A[t-1];!r||"."===r||".."===r||"**"===r||this.isWindows&&/^[a-z]:$/i.test(r)||(e=!0,A.splice(t-1,2),t-=2)}}while(e);return 0===A.length?[""]:A}
// First phase: single-pattern processing
// <pre> is 1 or more portions
// <rest> is 1 or more portions
// <p> is any portion other than ., .., '', or **
// <e> is . or ''
// **/.. is *brutal* for filesystem walking performance, because
// it effectively resets the recursive walk each time it occurs,
// and ** cannot be reduced out by a .. pattern part like a regexp
// or most strings (other than .., ., and '') can be.
// <pre>/**/../<p>/<p>/<rest> -> {<pre>/../<p>/<p>/<rest>,<pre>/**/<p>/<p>/<rest>}
// <pre>/<e>/<rest> -> <pre>/<rest>
// <pre>/<p>/../<rest> -> <pre>/<rest>
// **/**/<rest> -> **/<rest>
// **/*/<rest> -> */**/<rest> <== not valid because ** doesn't follow
// this WOULD be allowed if ** did follow symlinks, or * didn't
firstPhasePreProcess(A){let e=!1;do{e=!1;
// <pre>/**/../<p>/<p>/<rest> -> {<pre>/../<p>/<p>/<rest>,<pre>/**/<p>/<p>/<rest>}
for(let t of A){let r=-1;for(;-1!==(r=t.indexOf("**",r+1));){let n=r;for(;"**"===t[n+1];)
// <pre>/**/**/<rest> -> <pre>/**/<rest>
n++;
// eg, if gs is 2 and gss is 4, that means we have 3 **
// parts, and can remove 2 of them.
n>r&&t.splice(r+1,n-r);let s=t[r+1];const o=t[r+2],i=t[r+3];if(".."!==s)continue;if(!o||"."===o||".."===o||!i||"."===i||".."===i)continue;e=!0,
// edit parts in place, and push the new one
t.splice(r,1);const a=t.slice(0);a[r]="**",A.push(a),r--}
// <pre>/<e>/<rest> -> <pre>/<rest>
if(!this.preserveMultipleSlashes){for(let A=1;A<t.length-1;A++){const r=t[A];
// don't squeeze out UNC patterns
1===A&&""===r&&""===t[0]||("."!==r&&""!==r||(e=!0,t.splice(A,1),A--))}"."!==t[0]||2!==t.length||"."!==t[1]&&""!==t[1]||(e=!0,t.pop())}
// <pre>/<p>/../<rest> -> <pre>/<rest>
let n=0;for(;-1!==(n=t.indexOf("..",n+1));){const A=t[n-1];if(A&&"."!==A&&".."!==A&&"**"!==A){e=!0;const A=1===n&&"**"===t[n+1]?["."]:[];t.splice(n-1,2,...A),0===t.length&&t.push(""),n-=2}}}}while(e);return A}
// second phase: multi-pattern dedupes
// {<pre>/*/<rest>,<pre>/<p>/<rest>} -> <pre>/*/<rest>
// {<pre>/<rest>,<pre>/<rest>} -> <pre>/<rest>
// {<pre>/**/<rest>,<pre>/<rest>} -> <pre>/**/<rest>
// {<pre>/**/<rest>,<pre>/**/<p>/<rest>} -> <pre>/**/<rest>
// ^-- not valid because ** doens't follow symlinks
secondPhasePreProcess(A){for(let e=0;e<A.length-1;e++)for(let t=e+1;t<A.length;t++){const r=this.partsMatch(A[e],A[t],!this.preserveMultipleSlashes);if(r){A[e]=[],A[t]=r;break}}return A.filter(A=>A.length)}partsMatch(A,e,t=!1){let r=0,n=0,s=[],o="";for(;r<A.length&&n<e.length;)if(A[r]===e[n])s.push("b"===o?e[n]:A[r]),r++,n++;else if(t&&"**"===A[r]&&e[n]===A[r+1])s.push(A[r]),r++;else if(t&&"**"===e[n]&&A[r]===e[n+1])s.push(e[n]),n++;else if("*"!==A[r]||!e[n]||!this.options.dot&&e[n].startsWith(".")||"**"===e[n]){if("*"!==e[n]||!A[r]||!this.options.dot&&A[r].startsWith(".")||"**"===A[r])return!1;if("a"===o)return!1;o="b",s.push(e[n]),r++,n++}else{if("b"===o)return!1;o="a",s.push(A[r]),r++,n++}
// if we fall out of the loop, it means they two are identical
// as long as their lengths match
return A.length===e.length&&s}parseNegate(){if(this.nonegate)return;const A=this.pattern;let e=!1,t=0;for(let r=0;r<A.length&&"!"===A.charAt(r);r++)e=!e,t++;t&&(this.pattern=A.slice(t)),this.negate=e}
// set partial to true to test if, for example,
// "/a/b" matches the start of "/*/b/*/d"
// Partial means, if you run out of file before you run
// out of pattern, then that's fine, as long as all
// the parts match.
matchOne(A,e,t=!1){let r=0,n=0;
// UNC paths like //?/X:/... can match X:/... and vice versa
// Drive letters in absolute drive or unc paths are always compared
// case-insensitively.
if(this.isWindows){const t="string"==typeof A[0]&&/^[a-z]:$/i.test(A[0]),s=!t&&""===A[0]&&""===A[1]&&"?"===A[2]&&/^[a-z]:$/i.test(A[3]),o="string"==typeof e[0]&&/^[a-z]:$/i.test(e[0]),i=s?3:t?0:void 0,a=!o&&""===e[0]&&""===e[1]&&"?"===e[2]&&"string"==typeof e[3]&&/^[a-z]:$/i.test(e[3])?3:o?0:void 0;if("number"==typeof i&&"number"==typeof a){const[t,s]=[A[i],e[a]];
// start matching at the drive letter index of each
t.toLowerCase()===s.toLowerCase()&&(e[a]=t,n=a,r=i)}}
// resolve and reduce . and .. portions in the file as well.
// don't need to do the second phase, because it's only one string[]
const{optimizationLevel:s=1}=this.options;return s>=2&&(A=this.levelTwoFileOptimize(A)),e.includes(Ko)?this.#HA(A,e,t,r,n):this.#VA(A,e,t,r,n)}#HA(A,e,t,r,n){
// split the pattern into head, tail, and middle of ** delimited parts
const s=e.indexOf(Ko,n),o=e.lastIndexOf(Ko),[i,a,g]=t?[e.slice(n,s),e.slice(s+1),[]]:[e.slice(n,s),e.slice(s+1,o),e.slice(o+1)];
// check the head, from the current file/pattern index.
if(i.length){const e=A.slice(r,r+i.length);if(!this.#VA(e,i,t,0,0))return!1;r+=i.length,n+=i.length}
// now we know the head matches!
// if the last portion is not empty, it MUST match the end
// check the tail
let c=0;if(g.length){
// if head + tail > file, then we cannot possibly match
if(g.length+r>A.length)return!1;
// try to match the tail
let e=A.length-g.length;if(this.#VA(A,g,t,e,0))c=g.length;else{
// affordance for stuff like a/**/* matching a/b/
// if the last file portion is '', and there's more to the pattern
// then try without the '' bit.
if(""!==A[A.length-1]||r+g.length===A.length)return!1;if(e--,!this.#VA(A,g,t,e,0))return!1;c=g.length+1}}
// now we know the tail matches!
// the middle is zero or more portions wrapped in **, possibly
// containing more ** sections.
// so a/**/b/**/c/**/d has become **/b/**/c/**
// if it's empty, it means a/**/b, just verify we have no bad dots
// if there's no tail, so it ends on /**, then we must have *something*
// after the head, or it's not a matc
if(!a.length){let e=!!c;for(let t=r;t<A.length-c;t++){const r=String(A[t]);if(e=!0,"."===r||".."===r||!this.options.dot&&r.startsWith("."))return!1}
// in partial mode, we just need to get past all file parts
return t||e}
// now we know that there's one or more body sections, which can
// be matched anywhere from the 0 index (because the head was pruned)
// through to the length-fileTailMatch index.
// split the body up into sections, and note the minimum index it can
// be found at (start with the length of all previous segments)
// [section, before, after]
const Q=[[[],0]];let E=Q[0],B=0;const h=[0];for(const A of a)A===Ko?(h.push(B),E=[[],0],Q.push(E)):(E[0].push(A),B++);let I=Q.length-1;const C=A.length-c;for(const A of Q)A[1]=C-(h[I--]+A[0].length);return!!this.#xA(A,Q,r,0,t,0,!!c)}
// return false for "nope, not matching"
// return null for "not matching, cannot keep trying"
#xA(A,
// pattern section, last possible position for it
e,t,r,n,s,o){
// take the first body segment, and walk from fileIndex to its "after"
// value at the end
// If it doesn't match at that position, we increment, until we hit
// that final possible position, and give up.
// If it does match, then advance and try to rest.
// If any of them fail we keep walking forward.
// this is still a bit recursively painful, but it's more constrained
// than previous implementations, because we never test something that
// can't possibly be a valid matching condition.
const i=e[r];if(!i){
// just make sure that there's no bad dots
for(let e=t;e<A.length;e++){o=!0;const t=A[e];if("."===t||".."===t||!this.options.dot&&t.startsWith("."))return!1}return o}
// have a non-globstar body section to test
const[a,g]=i;for(;t<=g;){
// if limit exceeded, no match. intentional false negative,
// acceptable break in correctness for security.
if(this.#VA(A.slice(0,t+a.length),a,n,t,0)&&s<this.maxGlobstarRecursion){
// match! see if the rest match. if so, we're done!
const i=this.#xA(A,e,t+a.length,r+1,n,s+1,o);if(!1!==i)return i}const i=A[t];if("."===i||".."===i||!this.options.dot&&i.startsWith("."))return!1;t++}
// walked off. no point continuing
return n||null}#VA(A,e,t,r,n){let s,o,i,a;for(s=r,o=n,a=A.length,i=e.length;s<a&&o<i;s++,o++){this.debug("matchOne loop");let t,r=e[o],n=A[s];
// should be impossible.
// some invalid regexp stuff in the set.
/* c8 ignore start */
if(this.debug(e,r,n),!1===r||r===Ko)return!1;
/* c8 ignore stop */
// something other than **
// non-magic patterns just have to match exactly
// patterns with magic have been turned into regexps.
if("string"==typeof r?(t=n===r,this.debug("string match",r,n,t)):(t=r.test(n),this.debug("pattern match",r,n,t)),!t)return!1}
// Note: ending in / means that we'll get a final ""
// at the end of the pattern.  This can only match a
// corresponding "" at the end of the file.
// If the file ends in /, then it can only match a
// a pattern that ends in /, unless the pattern just
// doesn't have any more for it. But, a/b/ should *not*
// match "a/b/*", even though "" matches against the
// [^/]*? pattern, except in partial mode, where it might
// simply not be reached yet.
// However, a/b/ should still satisfy a/*
// now either we fell off the end of the pattern, or we're done.
if(s===a&&o===i)
// ran out of pattern and filename at the same time.
// an exact hit!
return!0;
/* c8 ignore stop */if(s===a)
// ran out of file, but still had pattern left.
// this is ok if we're doing the match as part of
// a glob fs traversal.
return t;if(o===i)
// ran out of pattern, still have file left.
// this is only acceptable if we're on the very last
// empty segment of a file with a trailing slash.
// a/* should match a/b/
return s===a-1&&""===A[s];
/* c8 ignore start */
// should be unreachable.
throw new Error("wtf?")}braceExpand(){return _o(this.pattern,this.options)}parse(A){to(A);const e=this.options;
// shortcuts
if("**"===A)return Ko;if(""===A)return"";
// far and away, the most common glob pattern parts are
// *, *.*, and *.<ext>  Add a fast check method for those.
let t,r=null;(t=A.match(vo))?r=e.dot?Ho:Jo:(t=A.match(bo))?r=(e.nocase?e.dot?Mo:No:e.dot?So:Fo)(t[1]):(t=A.match(Vo))?r=(e.nocase?e.dot?Wo:xo:e.dot?Oo:Po)(t):(t=A.match(Uo))?r=e.dot?To:Lo:(t=A.match(Go))&&(r=Yo);const n=ko.fromGlob(A,this.options).toMMPattern();return r&&"object"==typeof n&&
// Avoids overriding in frozen environments
Reflect.defineProperty(n,"test",{value:r}),n}makeRe(){if(this.regexp||!1===this.regexp)return this.regexp;
// at this point, this.set is a 2d array of partial
// pattern strings, or "**".

// It's better to use .match().  This function shouldn't
// be used, really, but it's pretty convenient sometimes,
// when you just want to work with a regex.
const A=this.set;if(!A.length)return this.regexp=!1,this.regexp;const e=this.options,t=e.noglobstar?"[^/]*?":e.dot?"(?:(?!(?:\\/|^)(?:\\.{1,2})($|\\/)).)*?":"(?:(?!(?:\\/|^)\\.).)*?",r=new Set(e.nocase?["i"]:[]);
// regexpify non-globstar patterns
// if ** is only item, then we just do one twoStar
// if ** is first, and there are more, prepend (\/|twoStar\/)? to next
// if ** is last, append (\/twoStar|) to previous
// if ** is in the middle, append (\/|\/twoStar\/) to previous
// then filter out GLOBSTAR symbols
let n=A.map(A=>{const e=A.map(A=>{if(A instanceof RegExp)for(const e of A.flags.split(""))r.add(e);return"string"==typeof A?A.replace(/[-[\]{}()*+?.,\\^$|#\s]/g,"\\$&"):A===Ko?Ko:A._src});e.forEach((A,r)=>{const n=e[r+1],s=e[r-1];A===Ko&&s!==Ko&&(void 0===s?void 0!==n&&n!==Ko?e[r+1]="(?:\\/|"+t+"\\/)?"+n:e[r]=t:void 0===n?e[r-1]=s+"(?:\\/|\\/"+t+")?":n!==Ko&&(e[r-1]=s+"(?:\\/|\\/"+t+"\\/)"+n,e[r+1]=Ko))});const n=e.filter(A=>A!==Ko);
// For partial matches, we need to make the pattern match
// any prefix of the full path. We do this by generating
// alternative patterns that match progressively longer prefixes.
if(this.partial&&n.length>=1){const A=[];for(let e=1;e<=n.length;e++)A.push(n.slice(0,e).join("/"));return"(?:"+A.join("|")+")"}return n.join("/")}).join("|");
// need to wrap in parens if we had more than one thing with |,
// otherwise only the first will be anchored to ^ and the last to $
const[s,o]=A.length>1?["(?:",")"]:["",""];
// must match entire pattern
// ending in a * or ** will make it less strict.
n="^"+s+n+o+"$",
// In partial mode, '/' should always match as it's a valid prefix for any pattern
this.partial&&(n="^(?:\\/|"+s+n.slice(1,-1)+o+")$"),
// can match anything, as long as it's not this.
this.negate&&(n="^(?!"+n+").+$");try{this.regexp=new RegExp(n,[...r].join(""));
/* c8 ignore start */}catch{
// should be impossible
this.regexp=!1}
/* c8 ignore stop */return this.regexp}slashSplit(A){
// if p starts with // on windows, we preserve that
// so that UNC paths aren't broken.  Otherwise, any number of
// / characters are coalesced into one, unless
// preserveMultipleSlashes is set to true.
return this.preserveMultipleSlashes?A.split("/"):this.isWindows&&/^\/\/[^/]+/.test(A)?["",...A.split(/\/+/)]:A.split(/\/+/)}match(A,e=this.partial){
// short-circuit in the case of busted things.
// comments, etc.
if(this.debug("match",A,this.pattern),this.comment)return!1;if(this.empty)return""===A;if("/"===A&&e)return!0;const t=this.options;
// windows: need to use /, not \
this.isWindows&&(A=A.split("\\").join("/"));
// treat the test path as a set of pathparts.
const r=this.slashSplit(A);this.debug(this.pattern,"split",r);
// just ONE of the pattern sets in this.set needs to match
// in order for it to be valid.  If negating, then just one
// match means that we have failed.
// Either way, return on the first hit.
const n=this.set;this.debug(this.pattern,"set",n);
// Find the basename of the path by looking for the last non-empty segment
let s=r[r.length-1];if(!s)for(let A=r.length-2;!s&&A>=0;A--)s=r[A];for(const A of n){let n=r;t.matchBase&&1===A.length&&(n=[s]);if(this.matchOne(n,A,e))return!!t.flipNegate||!this.negate}
// didn't get any hits.  this is success if it's a negative
// pattern, failure otherwise.
return!t.flipNegate&&this.negate}static defaults(A){return Ro.defaults(A).Minimatch}}
/* c8 ignore stop */Ro.AST=ko,Ro.Minimatch=$o,Ro.escape=(A,{windowsPathsNoEscape:e=!1,magicalBraces:t=!1}={})=>
// don't need to escape +@! because we escape the parens
// that make those magic, and escaping ! as [!] isn't valid,
// because [!]] is a valid glob class meaning not ']'.
t?e?A.replace(/[?*()[\]{}]/g,"[$&]"):A.replace(/[?*()[\]\\{}]/g,"\\$&"):e?A.replace(/[?*()[\]]/g,"[$&]"):A.replace(/[?*()[\]\\]/g,"\\$&"),Ro.unescape=io;const Ai="win32"===process.platform;
/**
 * Helper class for parsing paths into segments
 */class ei{
/**
     * Constructs a Path
     * @param itemPath Path or array of segments
     */
constructor(A){
// String
if(this.segments=[],"string"==typeof A)
// Not rooted
if(c(A,"Parameter 'itemPath' must not be empty"),hs(
// Normalize slashes and trim unnecessary trailing slash
A=Cs(A))){
// Add all segments, while not at the root
let e=A,t=Qs(e);for(;t!==e;){
// Add the segment
const A=s.basename(e);this.segments.unshift(A),
// Truncate the last segment
e=t,t=Qs(e)}
// Remainder is the root
this.segments.unshift(e)}else this.segments=A.split(s.sep);else{
// Must not be empty
c(A.length>0,"Parameter 'itemPath' must not be an empty array");
// Each segment
for(let e=0;e<A.length;e++){let t=A[e];
// Must not be empty
c(t,"Parameter 'itemPath' must not contain any empty segments"),
// Normalize slashes
t=Is(A[e]),
// Root segment
0===e&&hs(t)?(t=Cs(t),c(t===Qs(t),"Parameter 'itemPath' root segment contains information for multiple segments"),this.segments.push(t)):(
// Must not contain slash
c(!t.includes(s.sep),"Parameter 'itemPath' contains unexpected path separators"),this.segments.push(t))}}}
/**
     * Converts the path to it's string representation
     */toString(){
// First segment
let A=this.segments[0],e=A.endsWith(s.sep)||Ai&&/^[A-Z]:$/i.test(A);
// All others
for(let t=1;t<this.segments.length;t++)e?e=!1:A+=s.sep,A+=this.segments[t];return A}}const ti="win32"===process.platform;class ri{constructor(A,e=!1,t,r){
// Pattern overload
let n;if(
/**
         * Indicates whether matches should be excluded from the result set
         */
this.negate=!1,"string"==typeof A)n=A.trim();else{c((
// Convert to pattern
t=t||[]).length,"Parameter 'segments' must not empty");const e=ri.getLiteral(t[0]);c(e&&Bs(e),"Parameter 'segments' first element must be a root path"),n=new ei(t).toString().trim(),A&&(n=`!${n}`)}
// Negate
for(;n.startsWith("!");)this.negate=!this.negate,n=n.substr(1).trim();
// Normalize slashes and ensures absolute root
n=ri.fixupPattern(n,r),
// Segments
this.segments=new ei(n).segments,
// Trailing slash indicates the pattern should only match directories, not regular files
this.trailingSeparator=Is(n).endsWith(s.sep),n=Cs(n);
// Search path (literal path prior to the first glob segment)
let o=!1;const i=this.segments.map(A=>ri.getLiteral(A)).filter(A=>!o&&!(o=""===A));this.searchPath=new ei(i).toString(),
// Root RegExp (required when determining partial match)
this.rootRegExp=new RegExp(ri.regExpEscape(i[0]),ti?"i":""),this.isImplicitPattern=e;
// Create minimatch
const a={dot:!0,nobrace:!0,nocase:ti,nocomment:!0,noext:!0,nonegate:!0};n=ti?n.replace(/\\/g,"/"):n,this.minimatch=new $o(n,a)}
/**
     * Matches the pattern against the specified path
     */match(A){
// Match
// Last segment is globstar?
return"**"===this.segments[this.segments.length-1]?
// Append a trailing slash. Otherwise Minimatch will not match the directory immediately
// preceding the globstar. For example, given the pattern `/foo/**`, Minimatch returns
// false for `/foo` but returns true for `/foo/`. Append a trailing slash to handle that quirk.
(
// Normalize slashes
A=Is(A)).endsWith(s.sep)||!1!==this.isImplicitPattern||(
// Note, this is safe because the constructor ensures the pattern has an absolute root.
// For example, formats like C: and C:foo on Windows are resolved to an absolute root.
A=`${A}${s.sep}`):
// Normalize slashes and trim unnecessary trailing slash
A=Cs(A),this.minimatch.match(A)?this.trailingSeparator?ls.Directory:ls.All:ls.None}
/**
     * Indicates whether the pattern may match descendants of the specified path
     */partialMatch(A){
// matchOne does not handle root path correctly
return Qs(
// Normalize slashes and trim unnecessary trailing slash
A=Cs(A))===A?this.rootRegExp.test(A):this.minimatch.matchOne(A.split(ti?/\\+/:/\/+/),this.minimatch.set[0],!0)}
/**
     * Escapes glob patterns within a path
     */static globEscape(A){return(ti?A:A.replace(/\\/g,"\\\\")).replace(/(\[)(?=[^/]+\])/g,"[[]").replace(/\?/g,"[?]").replace(/\*/g,"[*]");// escape '*'
}
/**
     * Normalizes slashes and ensures absolute root
     */static fixupPattern(e,t){
// Empty
c(e,"pattern cannot be empty");
// Must not contain `.` segment, unless first segment
// Must not contain `..` segment
const r=new ei(e).segments.map(A=>ri.getLiteral(A));
// Replace leading `.` segment
if(c(r.every((A,e)=>("."!==A||0===e)&&".."!==A),`Invalid pattern '${e}'. Relative pathing '.' and '..' is not allowed.`),
// Must not contain globs in root, e.g. Windows UNC path \\foo\b*r
c(!hs(e)||r[0],`Invalid pattern '${e}'. Root segment must not contain globs.`),"."===(
// Normalize slashes
e=Is(e))||e.startsWith(`.${s.sep}`))e=ri.globEscape(process.cwd())+e.substr(1);else if("~"===e||e.startsWith(`~${s.sep}`))t=t||A.homedir(),c(t,"Unable to determine HOME directory"),c(Bs(t),`Expected HOME directory to be a rooted path. Actual '${t}'`),e=ri.globEscape(t)+e.substr(1);else if(ti&&(e.match(/^[A-Z]:$/i)||e.match(/^[A-Z]:[^\\]/i))){let A=Es("C:\\dummy-root",e.substr(0,2));e.length>2&&!A.endsWith("\\")&&(A+="\\"),e=ri.globEscape(A)+e.substr(2)}
// Replace relative root, e.g. pattern is \ or \foo
else if(ti&&("\\"===e||e.match(/^\\[^\\]/))){let A=Es("C:\\dummy-root","\\");A.endsWith("\\")||(A+="\\"),e=ri.globEscape(A)+e.substr(1)}
// Otherwise ensure absolute root
else e=Es(ri.globEscape(process.cwd()),e);return Is(e)}
/**
     * Attempts to unescape a pattern segment to create a literal path segment.
     * Otherwise returns empty string.
     */static getLiteral(A){let e="";for(let t=0;t<A.length;t++){const r=A[t];
// Escape
if("\\"===r&&!ti&&t+1<A.length)e+=A[++t];else{if("*"===r||"?"===r)return"";
// Append
if("["===r&&t+1<A.length){let r="",n=-1;for(let e=t+1;e<A.length;e++){const t=A[e];
// Escape
if("\\"===t&&!ti&&e+1<A.length)r+=A[++e];else{if("]"===t){n=e;break}
// Otherwise
r+=t}}
// Closed?
if(n>=0){
// Cannot convert
if(r.length>1)return"";
// Convert to literal
if(r){e+=r,t=n;continue}}
// Otherwise fall thru
}e+=r}}return e}
/**
     * Escapes regexp special characters
     * https://javascript.info/regexp-escaping
     */static regExpEscape(A){return A.replace(/[[\\^$.|?*+()]/g,"\\$&")}}class ni{constructor(A,e){this.path=A,this.level=e}}var si=function(A,e,t,r){return new(t||(t=Promise))(function(n,s){function o(A){try{a(r.next(A))}catch(A){s(A)}}function i(A){try{a(r.throw(A))}catch(A){s(A)}}function a(A){var e;A.done?n(A.value):(e=A.value,e instanceof t?e:new t(function(A){A(e)})).then(o,i)}a((r=r.apply(A,e||[])).next())})},oi=function(A){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e,t=A[Symbol.asyncIterator];return t?t.call(A):(A="function"==typeof __values?__values(A):A[Symbol.iterator](),e={},r("next"),r("throw"),r("return"),e[Symbol.asyncIterator]=function(){return this},e);function r(t){e[t]=A[t]&&function(e){return new Promise(function(r,n){(function(A,e,t,r){Promise.resolve(r).then(function(e){A({value:e,done:t})},e)})(r,n,(e=A[t](e)).done,e.value)})}}},ii=function(A){return this instanceof ii?(this.v=A,this):new ii(A)},ai=function(A,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var r,n=t.apply(A,e||[]),s=[];return r=Object.create(("function"==typeof AsyncIterator?AsyncIterator:Object).prototype),o("next"),o("throw"),o("return",function(A){return function(e){return Promise.resolve(e).then(A,g)}}),r[Symbol.asyncIterator]=function(){return this},r;function o(A,e){n[A]&&(r[A]=function(e){return new Promise(function(t,r){s.push([A,e,t,r])>1||i(A,e)})},e&&(r[A]=e(r[A])))}function i(A,e){try{(t=n[A](e)).value instanceof ii?Promise.resolve(t.value.v).then(a,g):c(s[0][2],t)}catch(A){c(s[0][3],A)}var t}function a(A){i("next",A)}function g(A){i("throw",A)}function c(A,e){A(e),s.shift(),s.length&&i(s[0][0],s[0][1])}};const gi="win32"===process.platform;class ci{constructor(A){this.patterns=[],this.searchPaths=[],this.options=gs(A)}getSearchPaths(){
// Return a copy
return this.searchPaths.slice()}glob(){return si(this,void 0,void 0,function*(){var A,e,t,r;const n=[];try{for(var s,o=!0,i=oi(this.globGenerator());!(A=(s=yield i.next()).done);o=!0){r=s.value,o=!1;const A=r;n.push(A)}}catch(A){e={error:A}}finally{try{o||A||!(t=i.return)||(yield t.call(i))}finally{if(e)throw e.error}}return n})}globGenerator(){return ai(this,arguments,function*(){
// Fill in defaults options
const A=gs(this.options),e=[];
// Implicit descendants?
for(const t of this.patterns)e.push(t),A.implicitDescendants&&(t.trailingSeparator||"**"!==t.segments[t.segments.length-1])&&e.push(new ri(t.negate,!0,t.segments.concat("**")));
// Push the search paths
const t=[];for(const A of ds(e)){ss(`Search path '${A}'`);
// Exists?
try{
// Intentionally using lstat. Detection for broken symlink
// will be performed later (if following symlinks).
yield ii(r.promises.lstat(A))}catch(A){if("ENOENT"===A.code)continue;throw A}t.unshift(new ni(A,1))}
// Search
const n=[];// used to detect cycles
for(;t.length;){
// Pop
const o=t.pop(),i=fs(e,o.path),a=!!i||ps(e,o.path);
// Match?
if(!i&&!a)continue;
// Stat
const g=yield ii(ci.stat(o,A,n));
// Broken symlink, or symlink cycle detected, or no longer exists
if(g&&(!A.excludeHiddenFiles||!s.basename(o.path).match(/^\./)))
// Directory
if(g.isDirectory()){
// Matched
if(i&ls.Directory&&A.matchDirectories)yield yield ii(o.path);else if(!a)continue;
// Push the child items in reverse
const e=o.level+1,n=(yield ii(r.promises.readdir(o.path))).map(A=>new ni(s.join(o.path,A),e));t.push(...n.reverse())}
// File
else i&ls.File&&(yield yield ii(o.path));
// Hidden file or directory?
}})}
/**
     * Constructs a DefaultGlobber
     */static create(A,e){return si(this,void 0,void 0,function*(){const t=new ci(e);gi&&(A=(A=A.replace(/\r\n/g,"\n")).replace(/\r/g,"\n"));const r=A.split("\n").map(A=>A.trim());for(const A of r)
// Empty or comment
A&&!A.startsWith("#")&&t.patterns.push(new ri(A));return t.searchPaths.push(...ds(t.patterns)),t})}static stat(A,e,t){return si(this,void 0,void 0,function*(){
// Note:
// `stat` returns info about the target of a symlink (or symlink chain)
// `lstat` returns info about a symlink itself
let n;if(e.followSymbolicLinks)try{
// Use `stat` (following symlinks)
n=yield r.promises.stat(A.path)}catch(t){if("ENOENT"===t.code){if(e.omitBrokenSymbolicLinks)return void ss(`Broken symlink '${A.path}'`);throw new Error(`No information found for the path '${A.path}'. This may indicate a broken symbolic link.`)}throw t}else
// Use `lstat` (not following symlinks)
n=yield r.promises.lstat(A.path);
// Note, isDirectory() returns false for the lstat of a symlink
if(n.isDirectory()&&e.followSymbolicLinks){
// Get the realpath
const e=yield r.promises.realpath(A.path);
// Fixup the traversal chain to match the item level
for(;t.length>=A.level;)t.pop();
// Test for a cycle
if(t.some(A=>A===e))return void ss(`Symlink cycle detected for path '${A.path}' and realpath '${e}'`);
// Update the traversal chain
t.push(e)}return n})}}var Qi=function(A,e,t,r){return new(t||(t=Promise))(function(n,s){function o(A){try{a(r.next(A))}catch(A){s(A)}}function i(A){try{a(r.throw(A))}catch(A){s(A)}}function a(A){var e;A.done?n(A.value):(e=A.value,e instanceof t?e:new t(function(A){A(e)})).then(o,i)}a((r=r.apply(A,e||[])).next())})};
/**
 * Constructs a globber
 *
 * @param patterns  Patterns separated by newlines
 * @param options   Glob options
 */function Ei(A,e){return Qi(this,void 0,void 0,function*(){return yield ci.create(A,e)})}
/**
 * 受 `log-level` 输入控制的简易日志器
 *
 * 设计要点:
 *   - 通过 `core.*` 写日志，以保留 GitHub Actions 的输出格式与注释、
 *     grouping 等特性
 *   - 按阈值过滤 `debug / info / warn`，`error` 直接放行（`error` 几乎
 *     总是代表"调用方不会绕过我们的失败路径"，让它到达用户即可；若
 *     被吞掉，反而会误导排障）
 *   - 以工厂 + 闭包形式提供，让调用点直接调用 `log.debug(...)`，与
 *     `console.*` / `core.*` 的写法几乎一致
 */const Bi={debug:10,info:20,warn:30,error:40};
/**
 * 构造一个受 `level` 阈值控制的 logger。
 *
 * 未识别的级别（理论上不会发生，因为 `resolveLogLevel` 已经归一化过）
 * 退化为 `info`，与 `resolveLogLevel` 的默认行为保持一致。
 */
/**
 * gist-kit-action 的共享类型定义
 */
const hi=["create","update","download","delete-files","delete-gist"],Ii=1e3;
/**
 * 公共工具：token 解析、输入解析、glob 展开以及所有操作共用的 HTTP 辅助函数
 *
 */
/** 用于重试退避的休眠辅助函数 */
function Ci(A){return new Promise(e=>setTimeout(e,A))}
/**
 * 对 `fetch` 进行的间接封装。我们刻意通过绑定到 `globalThis.fetch` 的
 * `const` 来调用，这样打包器/编译器就无法在静态分析时将调用点替换成
 * 模块作用域的绑定（那会破坏通过 `globalThis.fetch = jest.fn(...)` 进行
 * 测试 mock 的可行性）。
 */const li=globalThis;function ui(A,e){return li.fetch(A,e)}
/**
 * 按以下优先级获取 Gist 访问令牌:
 *   1. 显式传入的 `with.token` 输入
 *   2. `GITHUB_TOKEN` 环境变量
 *   3. `GH_TOKEN` 环境变量
 *
 * 当没有任何 token 时返回 `null`；由调用者决定当前操作是否必须使用 token
 *
 * 写操作总是需要 token, 只读的 `download` 不需要 token
 */
/**
 * 严格的布尔解析器: `true` 区分大小写，其他任何值一律为 false。
 */
function di(A){return"true"===A}
/**
 * 解析 `operation` 输入。空字符串回退到默认值（`download`）
 * 任何非空但不在允许列表中的值都会抛出异常，由分发器将 action 标记为失败
 */
/** 以跨平台的方式提取路径的最后一段 */
function fi(A){return N.basename(A)}
/**
 * 构造访问 GitHub REST API 所使用的请求头。当提供 `token` 时，请求会附上
 * 标准的 `Bearer` 授权头；否则为匿名请求（`download` 操作采用这种形式）。
 */function pi(A){const e={Accept:"application/vnd.github+json","X-GitHub-Api-Version":"2022-11-28"};return A&&(e.Authorization=`Bearer ${A}`),e}
/**
 * 当 API 没有返回 2xx 时，抛出包含响应体的友好错误。把响应体一并附上，
 * 在 Actions 日志中可直接看到 GitHub 给出的错误信息。
 */async function wi(A,e){const t=await A.text().catch(()=>"<unreadable body>");throw new Error(`GitHub API request failed (${e}): ${A.status} ${A.statusText}\n${t}`)}
/**
 * 向 GitHub REST API 发起的鉴权（或匿名）JSON 请求
 */async function yi(A,e){const t=await ui(`https://api.github.com${A}`,{method:e.method,headers:{...pi(e.token),...void 0!==e.body?{"Content-Type":"application/json"}:{}},body:void 0!==e.body?JSON.stringify(e.body):void 0});return t.ok||await wi(t,`${e.method} ${A}`),t}
/**
 * 下载 GitHub Gist 的原始文件内容
 *
 * 在任意失败时会重试，最多重试 {@link RAW_DOWNLOAD_RETRIES} 次，
 * 两次尝试之间间隔 {@link RAW_DOWNLOAD_RETRY_DELAY_MS} 毫秒。
 *
 * 将"最后一次尝试"计为第 (n+1) 次，因此 `RAW_DOWNLOAD_RETRIES = 3`
 * 表示"总共最多 3 次尝试"，而不是"在首次尝试之外再重试 3 次"。
 *
 * `log` 可选；未传时使用 `core.debug` 直写，行为与历史版本一致
 */async function Di(A,e=1,t={debug:ss,info:as,warn:is,error:os}){let r;try{r=await ui(A,{headers:{Accept:"application/octet-stream"}})}catch(r){if(e<3)return await Ci(Ii),Di(A,e+1,t);throw new Error(`Network error downloading ${A} after 3 attempts: ${r instanceof Error?r.message:String(r)}`,{cause:r})}return!r.ok&&e<3?(t.debug(`raw download attempt ${e} failed (${r.status}); retrying in 1000ms`),await Ci(Ii),Di(A,e+1,t)):(r.ok||await wi(r,`GET ${A}`),r)}
/**
 * 将 `log-level` 输入归一化为标准的小写值。未知值回退到 `info`
 */
/**
 * main.ts 负责:
 *
 *   1. 读取并解析 action 的输入参数
 *   2. 校验跨输入的依赖条件（例如 "create 操作需要 token"）
 *   3. 转发到对应的 `operations/*.ts` 模块
 *   4. 将最终的 `gist-id` 暴露为 action 输出
 *
 * 其他一切 HTTP 调用、文件 I/O、重试逻辑全部放在 `utils.ts` 或
 * 对应的具体操作模块中
 */
const mi={create:
/**
 * `create` 操作：以 `files: { <name>: { content } }` 调用 POST /gists
 */
async function(A,e){if(!A.filenames||0===A.filenames.length)throw new Error("`filenames` is required for the create operation");if(!A.token)throw new Error("A token with gist write permission is required for the create operation");e.debug(`Creating Gist with ${A.filenames.length} file(s)`);const t={};for(const e of A.filenames)t[fi(e)]={content:await M(e,"utf8")};const r={description:"Created by gist-kit-action",public:!0===A.createPublic,files:t},n=await yi("/gists",{method:"POST",token:A.token,body:r}),s=await n.json();return e.info(`Created Gist ${s.id}`),{newGistId:s.id}}
/**
 * `update` 操作：调用 PATCH /gists/{gist_id}
 *
 * GitHub PATCH 的语义：
 *   - `files` 中列出的文件会被 upsert（已存在则覆盖，新增则创建）。
 *   - 没有在 `files` 中列出的文件保持不变。
 *   - 若要从 Gist 中删除文件，将其值设为 `null`
 *
 * 这里仅支持 "upsert" 子集；删除操作由独立的 delete-files.ts 处理。
 */,update:async function(A,e){if(!A.gistId)throw new Error("`gist-id` is required for the update operation");if(!A.filenames||0===A.filenames.length)throw new Error("`filenames` is required for the update operation");if(!A.token)throw new Error("A token with gist write permission is required for the update operation");e.debug(`Updating Gist ${A.gistId} with ${A.filenames.length} file(s)`);const t={};for(const e of A.filenames)t[fi(e)]={content:await M(e,"utf8")};return await yi(`/gists/${encodeURIComponent(A.gistId)}`,{method:"PATCH",token:A.token,body:{files:t}}),e.info(`Updated Gist ${A.gistId}`),{}}
/**
 * `download` 步骤:
 * 1. 匿名调用 GET /gists/{gist_id} 拉取元数据
 * 2. 遍历返回的 `files` 字段
 * 3. 匿名拉取每个 `raw_url`
 * 4. 写入指定的目标路径
 *
 * 文件名语义（依据规范）：
 *   - `filenames` 可选。未提供时下载全部文件，写入当前工作目录下以其
 *     basename 命名的文件。
 *   - 提供时，则是期望的目标路径。去掉前面的目录部分（`basename`），
 *     用它与 Gist 中的文件名匹配。匹配成功后，将文件写入调用者提供的
 *     目标路径
 *     例: `/tmp/a.txt` 会写到 `/tmp/a.txt`，而不是 `./a.txt`
 *   - 未匹配上的会被警告并跳过。如果没有任何文件匹配成功，则让 action
 *     失败
 */,download:async function(A,e){if(!A.gistId)throw new Error("`gist-id` is required for the download operation");e.debug(`Fetching metadata for Gist ${A.gistId} (anonymous)`);
// 拉取 gist 元数据。API 允许匿名调用
const t=await yi(`/gists/${encodeURIComponent(A.gistId)}`,{method:"GET",token:null}),r=await t.json(),n=A.filenames&&A.filenames.length>0?A.filenames.map(A=>({destination:A,name:fi(A)})):Object.keys(r.files).map(A=>({destination:A,name:A}));let s=0;for(const{destination:t,name:o}of n){const n=r.files[o];if(!n){e.warn(`File "${o}" was requested but does not exist in Gist ${A.gistId}; skipping`);continue}s+=1,e.debug(`Downloading ${o} → ${t}`);const i=await Di(n.raw_url,1,e),a=await i.text();await U(N.dirname(t),{recursive:!0}),await L(t,a,"utf8")}if(n.length>0&&0===s)throw new Error(`None of the requested files matched any file in Gist ${A.gistId}`);return e.info(`Downloaded ${s} file(s) from Gist ${A.gistId}`+(s<n.length?` (${n.length-s} skipped)`:"")),{}},"delete-files":async function(A,e){if(!A.gistId)throw new Error("`gist-id` is required for the delete-files operation");if(!A.filenames||0===A.filenames.length)throw new Error("`filenames` is required for the delete-files operation");if(!A.token)throw new Error("A token with gist write permission is required for the delete-files operation");
// 防御性地丢弃空行
const t=A.filenames.filter(A=>A.length>0),r={};for(const A of t)r[A]=null;return e.debug(`Deleting ${t.length} file(s) from Gist ${A.gistId}: ${t.join(", ")}`),await yi(`/gists/${encodeURIComponent(A.gistId)}`,{method:"PATCH",token:A.token,body:{files:r}}),e.info(`Deleted ${t.length} file(s) from Gist ${A.gistId}`),{}},"delete-gist":async function(A,e){if(!A.gistId)throw new Error("`gist-id` is required for the delete-gist operation");if(!A.token)throw new Error("A token with gist write permission is required for the delete-gist operation");return e.debug(`Deleting Gist ${A.gistId}`),await yi(`/gists/${encodeURIComponent(A.gistId)}`,{method:"DELETE",token:A.token}),e.info(`Deleted Gist ${A.gistId}`),{}}};
/**
 * The entrypoint for the action. This file simply imports and runs the action's
 * main logic.
 */
/* istanbul ignore next */
!async function(){try{const A=ts("token"),e=ts("gist-id"),t=ts("operation"),r=ts("create-public-gist"),n=ts("filenames"),s=ts("log-level"),o=function(A){const e=A.trim();if(""===e)return"download";if(hi.includes(e))return e;throw new Error(`Invalid operation "${A}". Allowed: ${hi.join(", ")}`)}
/**
 * 将 `filenames` 输入（以换行分隔、可包含 glob 模式）展开为去重且排序后的
 * 本地文件路径列表。
 *
 * 输入为空时返回 `[]`；是否允许为空由调用者决定。
 */(t),i=function(A){if(A)return A;const e=process.env.GITHUB_TOKEN??process.env.GH_TOKEN;return e&&e.length>0?e:null}(A),a=function(A){switch(A){case"debug":case"info":case"warn":case"error":return A;default:return"info"}}(s),g=function(A){const e=Bi[A]??Bi.info;return{debug:A=>{e<=Bi.debug&&ss(A)},info:A=>{e<=Bi.info&&as(A)},warn:A=>{e<=Bi.warn&&is(A)},error:A=>{
// `error` 始终透传 —— 它影响 Actions 的 annotation，吞掉反而会
// 误导排障。
os(A)}}}(a);
// `filenames` 的语义随操作类型不同而不同：
//   - `create` / `update`：必填，作为本地文件系统的匹配模式
//     输入为空或没有匹配项则失败
//   - `delete-files`：必填，按字面量解析为要删除的 Gist 文件名列表，
//     不进行 glob 展开
//   - `download`：可选。输入为空表示"下载全部文件"；否则按
//     `basename` 与 Gist 文件名进行匹配，不展开本地文件系统
//   - `delete-gist`：忽略
let c,Q;switch(g.debug(`Resolved operation: ${o}`),g.debug(`Resolved log-level: ${a}`),g.debug(`Token present: ${null!==i}`),e&&g.debug(`Gist id: ${e}`),r&&g.debug(`create-public-gist=${r} → ${di(r)}`),o){case"create":case"update":{if(0===n.trim().length)throw new Error(`\`filenames\` is required for the ${o} operation`);const A=await async function(A,e){const t=A.split("\n").map(A=>A.trim()).filter(A=>A.length>0);if(0===t.length)return[];const r=await e(t.join("\n")),n=new Set;for await(const A of r.globGenerator())n.add(A);return[...n].sort()}(n,Ei);if(0===A.length)throw new Error(`\`filenames\` expanded to zero matches for the ${o} operation`);c=A;break}case"delete-files":{const A=n.split("\n").map(A=>A.trim()).filter(A=>A.length>0);if(0===A.length)throw new Error("`filenames` is required for the delete-files operation");c=A;break}case"download":n.trim().length>0&&(c=n.split("\n").map(A=>A.trim()).filter(A=>A.length>0));break;case"delete-gist":n.trim().length>0&&g.debug("Ignoring `filenames` for the delete-gist operation")}
// `create-public-gist` 仅对 create 操作有效。
"create"===o?Q=di(r):r.trim().length>0&&g.debug(`Ignoring \`create-public-gist\` for the ${o} operation`);const E={token:i??"",operation:o,logLevel:a,...e?{gistId:e}:{},...c?{filenames:c}:{},...void 0!==Q?{createPublic:Q}:{}},B=mi[o],h=await B(E,g);rs("gist-id",h.newGistId??e)}catch(A){
// 所有失败路径汇聚此处
A instanceof Error?ns(A):ns(String(A))}}();
//# sourceMappingURL=index.js.map
