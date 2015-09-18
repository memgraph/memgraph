#ifndef MEMGRAPH_SERVER_HTTP_STATUS_CODES_HPP
#define MEMGRAPH_SERVER_HTTP_STATUS_CODES_HPP

#include <map>
#include <string>

namespace http
{

#define HTTP_STATUS_CODES \
    CODE(Continue, 100, CONTINUE) \
    CODE(SwitchingProtocols, 101, SWITCHING PROTOCOLS) \
    CODE(Processing, 102, PROCESSING) \
    CODE(Ok, 200, OK) \
    CODE(Created, 201, CREATED) \
    CODE(Accepted, 202, ACCEPTED) \
    CODE(NonAuthoritativeInformation, 203, NON AUTHORITATIVE INFORMATION) \
    CODE(NoContent, 204, NO CONTENT) \
    CODE(ResetContent, 205, RESET CONTENT) \
    CODE(PartialContent, 206, PARTIAL CONTENT) \
    CODE(MultiStatus, 207, MULTI STATUS) \
    CODE(MultipleChoices, 300, MULTIPLE CHOICES) \
    CODE(MovedPermanently, 301, MOVED PERMANENTLY) \
    CODE(MovedTemporarily, 302, MOVED TEMPORARILY) \
    CODE(SeeOther, 303, SEE OTHER) \
    CODE(NotModified, 304, NOT MODIFIED) \
    CODE(UseProxy, 305, USE PROXY) \
    CODE(TemporaryRedirect, 307, TEMPORARY REDIRECT) \
    CODE(BadRequest, 400, BAD REQUEST) \
    CODE(Unauthorized, 401, UNAUTHORIZED) \
    CODE(PaymentRequired, 402, PAYMENT REQUIRED) \
    CODE(Forbidden, 403, FORBIDDEN) \
    CODE(NotFound, 404, NOT FOUND) \
    CODE(MethodNotAllowed, 405, METHOD NOT ALLOWED) \
    CODE(NotAcceptable, 406, NOT ACCEPTABLE) \
    CODE(ProxyAuthenticationRequired, 407, PROXY AUTHENTICATION REQUIRED) \
    CODE(RequestTimeOut, 408, REQUEST TIMEOUT) \
    CODE(Conflict, 409, CONFLICT) \
    CODE(Gone, 410, GONE) \
    CODE(LengthRequired, 411, LENGTH REQUIRED) \
    CODE(PreconditionFailed, 412, PRECONDITION FAILED) \
    CODE(RequestEntityTooLarge, 413, REQUEST ENTITY TOO LARGE) \
    CODE(RequestUriTooLarge, 414, REQUEST URI TOO LARGE) \
    CODE(UnsupportedMediaType, 415, UNSUPPORTED MEDIA TYPE) \
    CODE(RequestedRangeNotSatisfiable, 416, REQUESTED RANGE NOT SATISFIABLE) \
    CODE(ExpectationFailed, 417, EXPECTATION FAILED) \
    CODE(ImATeapot, 418, IM A TEAPOT) \
    CODE(UnprocessableEntity, 422, UNPROCESSABLE ENTITY) \
    CODE(Locked, 423, LOCKED) \
    CODE(FailedDependency, 424, FAILED DEPENDENCY) \
    CODE(UnorderedCollection, 425, UNORDERED COLLECTION) \
    CODE(UpgradeRequired, 426, UPGRADE REQUIRED) \
    CODE(PreconditionRequired, 428, PRECONDITION REQUIRED) \
    CODE(TooManyRequests, 429, TOO MANY REQUESTS) \
    CODE(RequestHeaderFieldsTooLarge, 431, REQUEST HEADER FIELDS TOO LARGE) \
    CODE(InternalServerError, 500, INTERNAL SERVER ERROR) \
    CODE(NotImplemented, 501, NOT IMPLEMENTED) \
    CODE(BadGateway, 502, BAD GATEWAY) \
    CODE(ServiceUnavailable, 503, SERVICE UNAVAILABLE) \
    CODE(GatewayTimeOut, 504, GATEWAY TIME OUT) \
    CODE(HttpVersionNotSupported, 505, HTTP VERSION NOT SUPPORTED) \
    CODE(VariantAlsoNegotiates, 506, VARIANT ALSO NEGOTIATES) \
    CODE(InsufficientStorage, 507, INSUFFICIENT STORAGE) \
    CODE(BandwidthLimitExceeded, 508, BANDWIDTH LIMIT EXCEEDED) \
    CODE(NotExtended, 510, NOT EXTENDED) \
    CODE(NetworkAuthenticationRequired, 511, NETWORK AUTHENTICATION REQUIRED)

enum Status
{
#define CODE(a, b, c) a = b,
    HTTP_STATUS_CODES
#undef CODE
};

static std::map<Status, std::string> to_string = {
#define CODE(a, b, c) { Status::a, #b " " #c },
    HTTP_STATUS_CODES
#undef CODE
};

}

#endif
