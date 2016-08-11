#include "io/network/tls.hpp"
#include "io/network/tls_error.hpp"

namespace io
{

Tls::Context::Context()
{
    auto method = SSLv23_server_method();
    ctx = SSL_CTX_new(method);

    if(!ctx)
    {
        ERR_print_errors_fp(stderr);
        throw io::TlsError("Unable to create TLS context");
    }

    SSL_CTX_set_ecdh_auto(ctx, 1);
}

Tls::Context::~Context()
{
    SSL_CTX_free(ctx);
}

Tls::Context& Tls::Context::cert(const std::string& path)
{
    if(SSL_CTX_use_certificate_file(ctx, path.c_str(), SSL_FILETYPE_PEM) >= 0)
        return *this;

    ERR_print_errors_fp(stderr);
    throw TlsError("Error Loading cert '{}'", path);
}

Tls::Context& Tls::Context::key(const std::string& path)
{
    if(SSL_CTX_use_PrivateKey_file(ctx, path.c_str(), SSL_FILETYPE_PEM) >= 0)
        return *this;

    ERR_print_errors_fp(stderr);
    throw TlsError("Error Loading private key '{}'", path);
}

void Tls::initialize()
{
    SSL_load_error_strings();
    OpenSSL_add_ssl_algorithms();
}

void Tls::cleanup()
{
    EVP_cleanup();
}

}
