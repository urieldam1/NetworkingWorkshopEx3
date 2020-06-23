cmake_minimum_required(VERSION 3.14.5)
project(ex2 C)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -libverbs")
set(CMAKE_C_STANDARD 11)
add_executable(ex2 kv_template.c HashMap.c HashMap.h msgType.h)


