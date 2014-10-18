SOURCES=src/mapreduce.erl src/mapreduce_sequential.erl src/demo_frequency.erl src/demo_grep.erl src/demo_inverted_index.erl
OBJECTS=$(SOURCES:.erl=.beam)

ebin:
	mkdir ebin

%.beam: %.erl ebin
	erlc +debug_info -o ebin $<

code: $(SOURCES) $(OBJECTS)
	cp -R test ebin

clean:
	rm -rf ebin
