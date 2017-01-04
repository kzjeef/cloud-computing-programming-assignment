cmake_minimum_required(VERSION 3.6)
project(mp1)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    Application.cpp
    Application.h
    EmulNet.cpp
    EmulNet.h
    Log.cpp
    Log.h
    Member.cpp
    Member.h
    MP1Node.cpp
    MP1Node.h
    Params.cpp
    Params.h
    Queue.h
    stdincludes.h)

add_executable(mp1 ${SOURCE_FILES})