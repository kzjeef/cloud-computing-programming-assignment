cmake_minimum_required(VERSION 3.6)
project(mp2_assignment)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    Application.cpp
    Application.h
    common.h
    EmulNet.cpp
    EmulNet.h
    Entry.cpp
    Entry.h
    HashTable.cpp
    HashTable.h
    Log.cpp
    Log.h
    Member.cpp
    Member.h
    Message.cpp
    Message.h
    MP1Node.cpp
    MP1Node.h
    MP2Node.cpp
    MP2Node.h
    Node.cpp
    Node.h
    Params.cpp
    Params.h
    Queue.h
    stdincludes.h
    Trace.cpp
    Trace.h)

add_executable(mp2_assignment ${SOURCE_FILES})