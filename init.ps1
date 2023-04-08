# TODO(gitbuda): All paths compiler paths are hardcoded -> CMake will handle.

$compiler="C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Tools\MSVC\14.30.30705\bin\Hostx64\x64\cl"
$stdlibinclude="C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Tools\MSVC\14.30.30705\include"
$toolsinclude="C:\Program Files (x86)\Windows Kits\10\Include\10.0.19041.0\ucrt"
$uminclude="C:\Program Files (x86)\Windows Kits\10\Include\10.0.19041.0\um"
$sharedinclude="C:\Program Files (x86)\Windows Kits\10\Include\10.0.19041.0\shared"
$stdlib="C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Tools\MSVC\14.30.30705\lib\x64"
$umlib="C:\Program Files (x86)\Windows Kits\10\Lib\10.0.19041.0\um\x64"
$ucrtlib="C:\Program Files (x86)\Windows Kits\10\Lib\10.0.19041.0\ucrt\x64"

& $compiler -I"$stdlibinclude" -I"$toolsinclude" -I"$uminclude" -I"$sharedinclude" -I"src" /std:c++20 /EHsc tests\manual\shared_library_handle.cpp /link /LIBPATH:"$stdlib" /LIBPATH:"$umlib" /LIBPATH:"$ucrtlib"
