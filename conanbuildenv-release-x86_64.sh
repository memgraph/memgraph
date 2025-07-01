script_folder="/home/mg/andi/memgraph"
echo "echo Restoring environment" > "$script_folder/deactivate_conanbuildenv-release-x86_64.sh"
for v in CXX CC PATH
do
    is_defined="true"
    value=$(printenv $v) || is_defined="" || true
    if [ -n "$value" ] || [ -n "$is_defined" ]
    then
        echo export "$v='$value'" >> "$script_folder/deactivate_conanbuildenv-release-x86_64.sh"
    else
        echo unset $v >> "$script_folder/deactivate_conanbuildenv-release-x86_64.sh"
    fi
done


export CXX="clang++-20"
export CC="clang-20"
export PATH="/home/mg/.conan2/p/cmake494b16f5f2445/p/bin:/home/mg/.conan2/p/ninjae5b5bd2e11aaa/p/bin:$PATH"
