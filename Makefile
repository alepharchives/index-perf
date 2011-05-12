
all: deps compile

compile:
	./rebar compile

deps:
	./rebar get-deps

clean: 
	./rebar clean


distclean: clean
	./rebar delete-deps

basho: deps
	@cd deps/basho_bench && make
	@cp deps/basho_bench/basho_bench .
	@cp deps/basho_bench/priv/*.r .
	@cp deps/basho_bench/include/basho_bench.hrl include
	@echo "You'll need to hack the header in compare.r to pick up common.r"


