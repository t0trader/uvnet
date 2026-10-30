#include "uvnet/error.hpp"

#if !defined(_WIN32)
#include <cerrno>
#include <netdb.h>
#endif

#include "detail/platform.hpp"

namespace uvnet {

std::string Error::message() const {
  switch (code) {
    case ErrorCode::None: return "success";
    case ErrorCode::InvalidArgument: return "invalid argument";
    case ErrorCode::OutOfMemory: return "out of memory";
    case ErrorCode::OutOfRange: return "out of range";
    case ErrorCode::PermissionDenied: return "permission denied";
    case ErrorCode::AddressInUse: return "address already in use";
    case ErrorCode::AddressNotAvailable: return "address not available";
    case ErrorCode::AddressFamilyNotSupported: return "address family not supported";
    case ErrorCode::AlreadyExists: return "already exists";
    case ErrorCode::AlreadyConnected: return "already connected";
    case ErrorCode::BadFileDescriptor: return "bad file descriptor";
    case ErrorCode::BrokenPipe: return "broken pipe";
    case ErrorCode::ConnectionAborted: return "connection aborted";
    case ErrorCode::ConnectionRefused: return "connection refused";
    case ErrorCode::ConnectionReset: return "connection reset by peer";
    case ErrorCode::ConnectionTimedOut: return "connection timed out";
    case ErrorCode::EndOfFile: return "end of file";
    case ErrorCode::HostNotFound: return "host not found";
    case ErrorCode::HostUnreachable: return "host unreachable";
    case ErrorCode::NetworkDown: return "network is down";
    case ErrorCode::NetworkUnreachable: return "network unreachable";
    case ErrorCode::Interrupted: return "interrupted system call";
    case ErrorCode::InProgress: return "operation in progress";
    case ErrorCode::MessageTooBig: return "message too big";
    case ErrorCode::NoBufferSpace: return "no buffer space available";
    case ErrorCode::NotConnected: return "socket not connected";
    case ErrorCode::NotSocket: return "not a socket";
    case ErrorCode::OperationNotSupported: return "operation not supported";
    case ErrorCode::ServiceNotSupported: return "service not supported";
    case ErrorCode::TimedOut: return "timed out";
    case ErrorCode::TryAgain: return "temporary failure, try again";
    case ErrorCode::WouldBlock: return "operation would block";
    case ErrorCode::Canceled: return "operation canceled";
    case ErrorCode::DnsFailure: return "dns resolution failed";
    case ErrorCode::NoData: return "no data";
    case ErrorCode::NotFound: return "not found";
    case ErrorCode::Unknown: return "unknown error";
  }
  return "unknown error";
}

namespace detail {

Error make_error(ErrorCode code, int native_code) noexcept {
  return Error{code, native_code};
}

#if defined(_WIN32)

int last_socket_error() noexcept {
  return WSAGetLastError();
}

Error from_socket_error(int code) noexcept {
  switch (code) {
    case 0: return Error{ErrorCode::None, 0};
    case WSAEACCES: return Error{ErrorCode::PermissionDenied, code};
    case WSAEADDRINUSE: return Error{ErrorCode::AddressInUse, code};
    case WSAEADDRNOTAVAIL: return Error{ErrorCode::AddressNotAvailable, code};
    case WSAEAFNOSUPPORT: return Error{ErrorCode::AddressFamilyNotSupported, code};
    case WSAEALREADY: return Error{ErrorCode::InProgress, code};
    case WSAECONNABORTED: return Error{ErrorCode::ConnectionAborted, code};
    case WSAECONNREFUSED: return Error{ErrorCode::ConnectionRefused, code};
    case WSAECONNRESET: return Error{ErrorCode::ConnectionReset, code};
    case WSAEDISCON: return Error{ErrorCode::ConnectionAborted, code};
    case WSAEDESTADDRREQ: return Error{ErrorCode::InvalidArgument, code};
    case WSAEFAULT: return Error{ErrorCode::InvalidArgument, code};
    case WSAEHOSTUNREACH: return Error{ErrorCode::HostUnreachable, code};
    case WSAEINTR: return Error{ErrorCode::Interrupted, code};
    case WSAEINVAL: return Error{ErrorCode::InvalidArgument, code};
    case WSAEISCONN: return Error{ErrorCode::AlreadyConnected, code};
    case WSAEMFILE: return Error{ErrorCode::NoBufferSpace, code};
    case WSAEMSGSIZE: return Error{ErrorCode::MessageTooBig, code};
    case WSAENETDOWN: return Error{ErrorCode::NetworkDown, code};
    case WSAENETUNREACH: return Error{ErrorCode::NetworkUnreachable, code};
    case WSAENOBUFS: return Error{ErrorCode::NoBufferSpace, code};
    case WSAENOTCONN: return Error{ErrorCode::NotConnected, code};
    case WSAENOTSOCK: return Error{ErrorCode::NotSocket, code};
    case WSAEOPNOTSUPP: return Error{ErrorCode::OperationNotSupported, code};
    case WSAEPROTONOSUPPORT: return Error{ErrorCode::OperationNotSupported, code};
    case WSAEPROTOTYPE: return Error{ErrorCode::OperationNotSupported, code};
    case WSAESHUTDOWN: return Error{ErrorCode::NotConnected, code};
    case WSAETIMEDOUT: return Error{ErrorCode::ConnectionTimedOut, code};
    case WSAEWOULDBLOCK: return Error{ErrorCode::WouldBlock, code};
    case ERROR_OPERATION_ABORTED: return Error{ErrorCode::Canceled, code};
    case ERROR_NOT_FOUND: return Error{ErrorCode::Canceled, code};
    case ERROR_NO_DATA: return Error{ErrorCode::NotConnected, code};
    // IOCP 完成包走 GetOverlappedResult，拿到的是 Win32 ERROR_* 而不是 WSAE_*。
    // 其中 UDP 收到的 ICMP 端口不可达就是以 ERROR_PORT_UNREACHABLE 形式上来的。
    case ERROR_PORT_UNREACHABLE: return Error{ErrorCode::ConnectionRefused, code};
    case ERROR_HOST_UNREACHABLE: return Error{ErrorCode::HostUnreachable, code};
    case ERROR_NETWORK_UNREACHABLE: return Error{ErrorCode::NetworkUnreachable, code};
    case ERROR_GRACEFUL_DISCONNECT: return Error{ErrorCode::ConnectionAborted, code};
    case ERROR_INVALID_HANDLE: return Error{ErrorCode::BadFileDescriptor, code};
    case ERROR_NETNAME_DELETED: return Error{ErrorCode::ConnectionReset, code};
    case ERROR_CONNECTION_ABORTED: return Error{ErrorCode::ConnectionAborted, code};
    case ERROR_CONNECTION_REFUSED: return Error{ErrorCode::ConnectionRefused, code};
    case ERROR_SEM_TIMEOUT: return Error{ErrorCode::TimedOut, code};
    case ERROR_FILE_NOT_FOUND: return Error{ErrorCode::NotFound, code};
    default: return Error{ErrorCode::Unknown, code};
  }
}

Error from_gai_error(int code) noexcept {
  switch (code) {
    case 0: return Error{ErrorCode::None, 0};
    case WSAHOST_NOT_FOUND: return Error{ErrorCode::HostNotFound, code};
    case WSATRY_AGAIN: return Error{ErrorCode::TryAgain, code};
    case WSANO_RECOVERY: return Error{ErrorCode::DnsFailure, code};
    case WSANO_DATA: return Error{ErrorCode::NoData, code};
    case WSAEAFNOSUPPORT: return Error{ErrorCode::AddressFamilyNotSupported, code};
    case WSAEINVAL: return Error{ErrorCode::InvalidArgument, code};
    case WSATYPE_NOT_FOUND: return Error{ErrorCode::ServiceNotSupported, code};
    case WSAESOCKTNOSUPPORT: return Error{ErrorCode::OperationNotSupported, code};
    default: return Error{ErrorCode::DnsFailure, code};
  }
}

#else

#include <cerrno>

int last_socket_error() noexcept {
  return errno;
}

Error from_socket_error(int code) noexcept {
  switch (code) {
    case 0: return Error{ErrorCode::None, 0};
    case EACCES: return Error{ErrorCode::PermissionDenied, code};
    case EPERM: return Error{ErrorCode::PermissionDenied, code};
    case EADDRINUSE: return Error{ErrorCode::AddressInUse, code};
    case EADDRNOTAVAIL: return Error{ErrorCode::AddressNotAvailable, code};
    case EAFNOSUPPORT: return Error{ErrorCode::AddressFamilyNotSupported, code};
    case EALREADY: return Error{ErrorCode::InProgress, code};
    case EBADF: return Error{ErrorCode::BadFileDescriptor, code};
    case ECANCELED: return Error{ErrorCode::Canceled, code};
    case ECONNABORTED: return Error{ErrorCode::ConnectionAborted, code};
    case ECONNREFUSED: return Error{ErrorCode::ConnectionRefused, code};
    case ECONNRESET: return Error{ErrorCode::ConnectionReset, code};
    case EDESTADDRREQ: return Error{ErrorCode::InvalidArgument, code};
    case EEXIST: return Error{ErrorCode::AlreadyExists, code};
    case EFAULT: return Error{ErrorCode::InvalidArgument, code};
    case EHOSTDOWN: return Error{ErrorCode::NetworkDown, code};
    case EHOSTUNREACH: return Error{ErrorCode::HostUnreachable, code};
    case EINPROGRESS: return Error{ErrorCode::InProgress, code};
    case EINTR: return Error{ErrorCode::Interrupted, code};
    case EINVAL: return Error{ErrorCode::InvalidArgument, code};
    case EISCONN: return Error{ErrorCode::AlreadyConnected, code};
    case EMSGSIZE: return Error{ErrorCode::MessageTooBig, code};
    case ENOBUFS: return Error{ErrorCode::NoBufferSpace, code};
    case ENOMEM: return Error{ErrorCode::OutOfMemory, code};
    case ENOENT: return Error{ErrorCode::NotFound, code};
    case ENETDOWN: return Error{ErrorCode::NetworkDown, code};
    case ENETUNREACH: return Error{ErrorCode::NetworkUnreachable, code};
    case ENOTCONN: return Error{ErrorCode::NotConnected, code};
    case ENOTSOCK: return Error{ErrorCode::NotSocket, code};
    case EOPNOTSUPP: return Error{ErrorCode::OperationNotSupported, code};
    case EPIPE: return Error{ErrorCode::BrokenPipe, code};
    case EPROTONOSUPPORT: return Error{ErrorCode::OperationNotSupported, code};
    case ETIMEDOUT: return Error{ErrorCode::TimedOut, code};
    case EWOULDBLOCK: return Error{ErrorCode::WouldBlock, code};
    default: return Error{ErrorCode::Unknown, code};
  }
}

Error from_gai_error(int code) noexcept {
  switch (code) {
    case 0: return Error{ErrorCode::None, 0};
    case EAI_AGAIN: return Error{ErrorCode::TryAgain, code};
    case EAI_BADFLAGS: return Error{ErrorCode::InvalidArgument, code};
    case EAI_FAIL: return Error{ErrorCode::DnsFailure, code};
    case EAI_FAMILY: return Error{ErrorCode::AddressFamilyNotSupported, code};
    case EAI_MEMORY: return Error{ErrorCode::OutOfMemory, code};
    case EAI_NONAME: return Error{ErrorCode::HostNotFound, code};
    case EAI_SERVICE: return Error{ErrorCode::ServiceNotSupported, code};
    case EAI_SOCKTYPE: return Error{ErrorCode::OperationNotSupported, code};
#if defined(EAI_ADDRFAMILY) && (EAI_ADDRFAMILY != EAI_FAMILY)
    case EAI_ADDRFAMILY: return Error{ErrorCode::AddressFamilyNotSupported, code};
#endif
#if defined(EAI_NODATA) && (EAI_NODATA != EAI_NONAME)
    case EAI_NODATA: return Error{ErrorCode::NoData, code};
#endif
    default: return Error{ErrorCode::DnsFailure, code};
  }
}

#endif

} // namespace detail
} // namespace uvnet
