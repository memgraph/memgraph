#pragma once

#include <map>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <glog/logging.h>

#include <libpq-fe.h>

#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "utils/exceptions.hpp"

using communication::bolt::QueryData;

namespace postgres {

class ClientException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

class ClientQueryException : public ClientException {
 public:
  using ClientException::ClientException;
  ClientQueryException() : ClientException("Couldn't execute query!") {}
};

class Client {
 public:
  Client(std::string &host, std::string &port, std::string &username,
         std::string &password, std::string database = "") {
    // https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    std::string pass = "";
    if (password != "") {
      pass = "password=" + password;
    }
    std::string conninfo =
        fmt::format("host={} port={} user={} {} dbname={} sslmode=disable",
                    host, port, username, pass, database);

    // Make a connection to the database.
    connection_ = PQconnectdb(conninfo.c_str());

    // Check to see that the backend connection was successfully made
    if (PQstatus(connection_) != CONNECTION_OK) {
      throw ClientException(PQerrorMessage(connection_));
    }
  }

  QueryData Execute(const std::string &query,
                    const std::map<std::string, std::string> &parameters) {
    QueryData ret;

    DLOG(INFO) << "Sending run message with statement: '" << query << "'";

    result_ = PQexec(connection_, query.c_str());
    if (PQresultStatus(result_) == PGRES_TUPLES_OK) {
      // get fields
      int num_fields = PQnfields(result_);
      for (int i = 0; i < num_fields; ++i) {
        ret.fields.push_back(PQfname(result_, i));
      }

      // get records
      int num_records = PQntuples(result_);
      ret.records.resize(num_records);
      for (int i = 0; i < num_records; ++i) {
        for (int j = 0; j < num_fields; ++j) {
          ret.records[i].push_back(std::string(PQgetvalue(result_, i, j)));
        }
      }

      // get metadata
      ret.metadata.insert({"status", std::string(PQcmdStatus(result_))});
      ret.metadata.insert({"rows_affected", std::string(PQcmdTuples(result_))});
    } else if (PQresultStatus(result_) != PGRES_COMMAND_OK) {
      throw ClientQueryException(PQerrorMessage(connection_));
    }

    PQclear(result_);
    result_ = nullptr;

    return ret;
  }

  void Close() {
    if (result_ != nullptr) {
      PQclear(result_);
      result_ = nullptr;
    }
    if (connection_ != nullptr) {
      PQfinish(connection_);
      connection_ = nullptr;
    }
  }

  ~Client() { Close(); }

 private:
  PGconn *connection_{nullptr};
  PGresult *result_{nullptr};
};
}
