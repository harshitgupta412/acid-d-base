typedef char byte;
#define MAX_STR_LEN 50
int
EncodeInt(int i, byte *bytes);
int
DecodeInt(byte *bytes);

int
EncodeCString(char *str, byte *bytes, int max_len);
int
DecodeCString(byte *bytes, char **str, int max_len);