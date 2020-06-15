//
// Created by l33t on 15/06/2020.
//

#ifndef EX2_MSGTYPE_H
#define EX2_MSGTYPE_H

#include <stdint.h>

enum msgType {
    EAGER_GET_REQUEST,
    EAGER_GET_RESPONSE,
    EAGER_SET_REQUEST,
    EAGER_SET_RESPONSE,
    RENDEZVOUS_GET_REQUEST,
    RENDEZVOUS_GET_RESPONSE,
    RENDEZVOUS_SET_REQUEST,
    RENDEZVOUS_SET_RESPONSE
};

struct msg{
    enum msgType type;
    const char* key;
    const char* value;
};



#endif //EX2_MSGTYPE_H


