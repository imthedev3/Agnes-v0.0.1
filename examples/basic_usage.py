from agnes import AgnesEngine, EngineConfig

async def main():
    # Create configuration
    config = EngineConfig(
        memory_size=1000,
        max_planning_depth=5,
        safety_checks=["input", "output", "resources"],
        neural_models={"default": "gpt3"},
        compute_resources={"max_memory": "8GB"}
    )
    
    # Initialize engine
    engine = AgnesEngine(config)
    
    # Process sample data
    result = await engine.process({
        "task": "analyze",
        "data": "Sample text for analysis"
    })
    
    print(result)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
