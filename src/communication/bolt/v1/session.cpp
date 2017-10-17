#include "communication/bolt/v1/session.hpp"

// Danger: If multiple sessions are associated with one worker nonactive
// sessions could become blocked for FLAGS_session_inactivity_timeout time.
// TODO: We should never associate more sessions with one worker.
DEFINE_int32(session_inactivity_timeout, 1800,
             "Time in seconds after which inactive sessions will be closed");
