#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include "codec.h"

typedef union {
    int i;
    byte  bytes[4];
} IntBytes;

int
EncodeInt(int i, byte *bytes) {
    IntBytes ib;
    ib.i = i;
    memcpy(bytes, ib.bytes, 4);
    return 4;
}

int
DecodeInt(byte *bytes) {
    IntBytes ib;
    memcpy(ib.bytes, bytes, 4);
    return ib.i;
}

/*
  Copies a null-terminated string to a <len><bytes> format. The length 
  is encoded as a 4-byte short. 
  Precondition: the 'bytes' buffer must have max_len bytes free.
  Returns the total number of bytes encoded (including the length)
 */
int
EncodeCString(char *str, byte *bytes, int max_len) {
    int len = strlen(str);
    if (len + 4 > max_len) {
	len = max_len - 4;
    }
    EncodeInt(len, bytes);
    memcpy(bytes+4, str, len);
    return len+4;
}

int
DecodeCString(byte *bytes, char ** str, int max_len) {
    int len = DecodeInt(bytes);
    *str = (char*) malloc((1+len)*sizeof(char));
    memcpy(*str, bytes+4, len);
    (*str)[len] = '\0';
    return len;
}