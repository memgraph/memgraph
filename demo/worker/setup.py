# -*- coding: utf-8 -*-

from distutils.core import setup, Extension
from Cython.Distutils import build_ext

setup(
    name='Benchmark client',
    ext_modules=[
        Extension(
            name='benchmark',
            sources=['benchmark.pyx'],
            extra_compile_args=['-O2', '-std=c++14'],
            include_dirs=["../../"],
            language='c++')
    ],
    cmdclass={'build_ext': build_ext}
)
