all: project

project: kv_template.c
	gcc HashMap.c HashMap.h kv_template.c -libverbs -o server && ln -sf server client

tar:
	tar -cvzf 315441683_206120537_204910681.tgz kv_template.c HashMap.c HashMap.h msgType.h makefile client server

clean:
	rm -f client server

#     rm -rf server client && gcc HashMap.c HashMap.h bw_template.c -libverbs -o server && ln -s server client