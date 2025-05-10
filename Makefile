.PHONY: bench
bench:
	@go test -bench=. -benchmem

.PHONY: ut
ut:
	@go test ./...
