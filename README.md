# streamduck

Steps to run:

1. Clone the repository.
2. Copy `.env.example` to `.env` and fill it out. The pipeline relies on the Azure Default Credential chain, so make sure you are logged in with `az login` (or equivalent) and have access to the target Event Hubs namespace.
3. Run `uv run python src/main.py validate-config` to verify configuration and credentials.
4. Run `uv run python src/main.py run` to start the pipeline.

## Architecture

```mermaid
flowchart LR
	subgraph Azure
		az[Azure Identity
		DefaultAzureCredential]
		EH[Event Hub Namespace
		(topics/partitions)]
	end

	subgraph StreamDuck
		CLI[Typer CLI
		uv run commands]
		CFG[Config Loader
		.env + Pydantic]
		ORCH[Async Orchestrator
		mapping manager]
		CONS[EventHub Consumer
		batching + checkpoints]
	end

	subgraph MotherDuck
		ING[MotherDuck Writer
		batch ingestion]
		CTRL[Control Table
		waterlevel checkpoints]
	end

	az --> EH
	CLI --> CFG --> ORCH
	CONS --> ORCH
	EH --> CONS
	ORCH --> ING
	ORCH --> CTRL
```
