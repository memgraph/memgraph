#pragma once

#include <memory>
#include <uv.h>

#include "uv_error.hpp"

namespace uv
{

class UvLoop final
{
public:
    using sptr = std::shared_ptr<UvLoop>;

    enum Mode {
        Default = UV_RUN_DEFAULT,
        Once = UV_RUN_ONCE,
        NoWait = UV_RUN_NOWAIT
    };

    UvLoop()
    {
        uv_loop = uv_default_loop();

        if(uv_loop == nullptr)
            throw UvError("Failed to initialize libuv event loop");
    }

    ~UvLoop()
    {
        uv_loop_close(uv_loop);
    }

    bool run(Mode mode)
    {
        return uv_run(uv_loop, static_cast<uv_run_mode>(mode));
    }

    bool alive()
    {
        return uv_loop_alive(uv_loop);
    }

    void stop()
    {
        uv_stop(uv_loop);
    }

    operator uv_loop_t*()
    {
        return uv_loop;
    }

private:
    uv_loop_t* uv_loop;
};

}
