REBAR = $(CURDIR)/rebar3

$(REBAR):
	wget https://s3.amazonaws.com/rebar3/rebar3 && chmod +x rebar3

.PHONY: ct
ct: $(REBAR)
	$(REBAR) ct --name 'test@127.0.0.1' --readable true -v -c

.PHONY: ct-suite
ct-suite: $(REBAR)
	$(REBAR) ct --name 'test@127.0.0.1' --readable true -v -c --suite $(SUITE)

.PHONY: cover
cover: ct
	$(REBAR) cover

.PHONY: fmt
fmt: $(REBAR)
	$(REBAR) fmt


