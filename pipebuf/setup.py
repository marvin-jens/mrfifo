# setup.py
from setuptools import setup, Extension
from Cython.Build import cythonize

ext = Extension(
    name="pipebuf_futex",
    sources=["pipebuf_futex.pyx"],
    extra_compile_args=["-std=c11", "-O2"],
    libraries=[],  # no special libs
)

setup(
    name="pipebuf_futex",
    ext_modules=cythonize([ext], language_level=3),
)