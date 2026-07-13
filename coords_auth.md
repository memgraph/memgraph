One thing is how to authenticate routing connection the other, if we're logging in only to the coord.
TODO: check Clickhouse


There is Bolt message LOGON:
- the scheme `basic` requires a username `principal: String` and a password `credentials: String`
- the scheme `bearer` requires only a token `credentials: String`

v1/states/init.hpp:
- BasicAuthentication carries principal and credentials
- SSOAuthentication carries credentials + scheme
 - SSO needs a bunch of env variables, where do they need to be set-up?

- AuthenticateUser, checks if coordinator and just ignores the request


If we forward the request from coords to data instances, do we always protect the communication?
All good if users enable intra-cluster TLS but what if it's not enabled?

We send the request from coord to any data instance? Main and replica can both authenticate
If the network is down, we would be unable to access coordinator => risky.


TODO:
- hide password when creating users [Run - memgraph] 'create user test identified by 'test''
- question for Ivan: is v1/states/init.hpp the only entrypoint for auth?
- currently coordinator query requires coordinator privilege...


We don't need user profiles.
We already have roles on coords


Is it okay to not authenticate the routing request?
We would disable all parts that require storage access.

### SSO with Ivan

- driver creates SSO connection
- you only need roles, not users
- --auth-module-mappings
- SSOAuthentication function, it returns user with role and user is returned from the external module
- env variables will also need to be supported on coords
- same SSO config needs to be set on coords and data instances
- Lab sends the request to the provider and Lab then sends fetched JWT to Memgraph
- access token: only for authorization
- id token: only for authentication
- we send over Bolt custom scheme
- this is OIDC

### SAML

- Used by Capitec
- in theory should be the same as for OIDC


### Kerberos

- env varijable
- Lab uopce nema Kerberos
- same as SSO


TODO: Think about reusing (de)serialization from the current RPC stack for coordinator logs
roles: admin, developer, readonly, service account

admin, developer: write access


TODO: License setting
The default should stay the same as is now (no auth)
The question is what to do with RELOAD_TLS privilege and COORDINATOR privilege.
Make sure to remove EnableWritingOnMain

Question is what to do about privileges?
